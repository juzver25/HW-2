"""Generate user events into the events topic.

Each producer owns a disjoint set of user_id values, so seq for a user is a
gap-free sequence defined by exactly one process. acks=all and idempotence are
required: a write is acknowledged only when every in-sync replica has it, and
broker retries do not duplicate records.
"""

import json
import logging
import os
import queue
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("producer")

EVENT_TYPES = ("page_view", "add_to_cart", "purchase")
TOPIC = os.environ.get("TOPIC", "events")
PRODUCER_ID = os.environ["PRODUCER_ID"]
USERS_PER_PRODUCER = int(os.environ.get("USERS_PER_PRODUCER", "4"))
EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "8"))
POISON_RATE = float(os.environ.get("POISON_RATE", "0.02"))
KEY_MODE = os.environ.get("KEY_MODE", "user_id")
STATE_PATH = os.environ.get("STATE_PATH", f"/state/producer-{PRODUCER_ID}.json")

if KEY_MODE not in ("user_id", "none"):
    raise SystemExit(f"KEY_MODE must be user_id or none, got {KEY_MODE!r}")

stopping = False
retry_q: queue.Queue[bytes] = queue.Queue()
delivered = 0
failed_deliveries = 0


def handle_signal(signum, _frame):
    global stopping
    log.info("signal %s, stopping after the local buffer is flushed", signum)
    stopping = True


def on_delivery(err, msg):
    global delivered, failed_deliveries
    if err is not None:
        failed_deliveries += 1
        log.error(
            "send failed topic=%s partition=%s key=%s error=%s",
            msg.topic(),
            msg.partition(),
            msg.key(),
            err,
        )
        # Same bytes, same event_id and seq: a later success does not open a gap.
        retry_q.put(msg.value())
        return
    delivered += 1
    if delivered % 50 == 0:
        log.info(
            "delivered=%s failed_attempts=%s last_partition=%s",
            delivered,
            failed_deliveries,
            msg.partition(),
        )


def on_error(err):
    log.error("kafka client error: %s", err)


def build_event(user_id: str, seq: int) -> bytes:
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "type": random.choice(EVENT_TYPES),
        "seq": seq,
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "producer_id": PRODUCER_ID,
            "poison": random.random() < POISON_RATE,
        },
    }
    return json.dumps(event, separators=(",", ":")).encode()


def message_key(user_id: str):
    if KEY_MODE == "none":
        return None
    return user_id.encode()


def load_seqs(user_ids: list[str]) -> dict[str, int]:
    try:
        with open(STATE_PATH, encoding="utf-8") as fh:
            saved = json.load(fh)
    except FileNotFoundError:
        saved = {}
    return {user_id: int(saved.get(user_id, 1)) for user_id in user_ids}


def save_seqs(seqs: dict[str, int]) -> None:
    directory = os.path.dirname(STATE_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(seqs, fh)
    os.replace(tmp, STATE_PATH)


def enqueue(producer: Producer, raw: bytes, user_id: str) -> bool:
    try:
        producer.produce(
            TOPIC,
            value=raw,
            key=message_key(user_id),
            on_delivery=on_delivery,
        )
        return True
    except BufferError:
        log.error("local producer buffer is full, backing off")
        producer.poll(0.5)
        return False


def main():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    user_ids = [f"user-{PRODUCER_ID}-{i}" for i in range(USERS_PER_PRODUCER)]
    next_seq = load_seqs(user_ids)
    log.info(
        "start producer=%s users=%s rate=%s/s poison_rate=%s key_mode=%s next_seq=%s",
        PRODUCER_ID,
        user_ids,
        EVENTS_PER_SECOND,
        POISON_RATE,
        KEY_MODE,
        next_seq,
    )

    producer = Producer(
        {
            "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
            "client.id": f"producer-{PRODUCER_ID}",
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "retries": 2147483647,
            "retry.backoff.ms": 200,
            # Keep records across a broker outage long enough to retry after recovery.
            "delivery.timeout.ms": 300000,
            "linger.ms": 5,
            "socket.timeout.ms": 10000,
            "log_level": 4,
            # Keyed records stay on one partition (murmur2). A null key is random,
            # which is what the KEY_MODE=none experiment needs.
            "partitioner": "murmur2_random",
            "error_cb": on_error,
        },
        logger=log,
    )

    interval = 1.0 / EVENTS_PER_SECOND
    next_tick = time.monotonic()
    user_cursor = 0

    while not stopping:
        producer.poll(0)

        if not retry_q.empty():
            raw = retry_q.get()
            user_id = json.loads(raw)["user_id"]
            if not enqueue(producer, raw, user_id):
                retry_q.put(raw)
            time.sleep(0.2)
            continue

        now = time.monotonic()
        if now < next_tick:
            time.sleep(min(0.05, next_tick - now))
            continue

        user_id = user_ids[user_cursor % len(user_ids)]
        user_cursor += 1
        seq = next_seq[user_id]
        raw = build_event(user_id, seq)
        if not enqueue(producer, raw, user_id):
            continue
        next_seq[user_id] = seq + 1
        save_seqs(next_seq)
        next_tick += interval

    log.info("flushing buffered records before exit")
    remaining = 1
    for _ in range(6):
        while not retry_q.empty():
            raw = retry_q.get()
            user_id = json.loads(raw)["user_id"]
            if not enqueue(producer, raw, user_id):
                retry_q.put(raw)
                break
        remaining = producer.flush(10)
        producer.poll(0)
        if remaining == 0 and retry_q.empty():
            break
        log.error("flush still waiting for %s records, retry_queue=%s", remaining, retry_q.qsize())
    if remaining or not retry_q.empty():
        log.error(
            "shutdown finished with undelivered records remaining=%s retry_queue=%s",
            remaining,
            retry_q.qsize(),
        )
    log.info(
        "stopped delivered=%s failed_attempts=%s next_seq=%s",
        delivered,
        failed_deliveries,
        next_seq,
    )


if __name__ == "__main__":
    main()
