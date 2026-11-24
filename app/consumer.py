"""Consumer group events-processor.

Offsets are committed only after the event is stored in the sink or, for a
poison event, after it has been copied to events.dlt. That is at-least-once:
a crash between the write and the commit redelivers the record, and the sink
ignores the duplicate event_id.

auto.offset.reset=earliest is deliberate. The group has no committed offset
on the first start, and producers may already have written events. "latest"
would skip that backlog. After the first commit the setting is not used.

Successful offsets are committed on a short interval and on rebalance, not
before the sink write. kill -9 skips the rebalance callback, so the last
interval of already-written events is redelivered and counted as duplicates.
"""

import json
import logging
import os
import signal
import time

import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("consumer")

TOPIC = os.environ.get("TOPIC", "events")
DLT_TOPIC = os.environ.get("DLT_TOPIC", "events.dlt")
CONSUMER_ID = os.environ["CONSUMER_ID"]
GROUP_ID = os.environ.get("GROUP_ID", "events-processor")
PROCESSING_DELAY_MS = int(os.environ.get("PROCESSING_DELAY_MS", "50"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "3"))
COMMIT_INTERVAL_MS = int(os.environ.get("COMMIT_INTERVAL_MS", "8000"))

stopping = False


def handle_signal(signum, _frame):
    global stopping
    log.info("signal %s, committing processed offsets and stopping", signum)
    stopping = True


class Sink:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = self._connect()

    def _connect(self):
        conn = psycopg.connect(self.dsn, autocommit=True)
        log.info("connected to sink")
        return conn

    def reconnect(self):
        try:
            self.conn.close()
        except psycopg.Error:
            pass
        self.conn = self._connect()

    def store(self, event: dict, topic: str, partition: int, offset: int) -> bool:
        """Insert the event. Return True if it was new, False if event_id already existed."""
        sql = """
        WITH inserted AS (
            INSERT INTO processed_events (
                event_id, user_id, seq, processor_id, topic, partition, kafka_offset
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
        ),
        bump AS (
            UPDATE dedup_stats
            SET dropped_duplicates = dropped_duplicates + 1
            WHERE id = 1
              AND NOT EXISTS (SELECT 1 FROM inserted)
            RETURNING dropped_duplicates
        )
        SELECT EXISTS (SELECT 1 FROM inserted) AS inserted
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        event["event_id"],
                        event["user_id"],
                        event["seq"],
                        f"consumer-{CONSUMER_ID}",
                        topic,
                        partition,
                        offset,
                    ),
                )
                inserted = cur.fetchone()[0]
        except psycopg.Error:
            log.exception("sink write failed, will retry the event")
            self.reconnect()
            raise
        if not inserted:
            log.info(
                "duplicate dropped event_id=%s user=%s seq=%s partition=%s offset=%s",
                event["event_id"],
                event["user_id"],
                event["seq"],
                partition,
                offset,
            )
        return inserted


class Processor:
    def __init__(self, consumer: Consumer, dlt: Producer, sink: Sink):
        self.consumer = consumer
        self.dlt = dlt
        self.sink = sink
        # (topic, partition) -> next offset to commit (last handled offset + 1)
        self.next_offset: dict[tuple[str, int], int] = {}
        self.dirty = False
        self.last_commit = time.monotonic()
        self.handled = 0
        self.duplicates = 0
        self.dlt_sent = 0
        self.log_resume = False

    def on_assign(self, _consumer, partitions):
        # No network calls here: this runs inside poll() and a blocking
        # OffsetFetch would deadlock the client thread.
        log.info(
            "REBALANCE assigned %s",
            [f"{part.topic}[{part.partition}]" for part in partitions],
        )
        self.log_resume = True

    def on_revoke(self, consumer, partitions):
        names = [f"{p.topic}[{p.partition}]" for p in partitions]
        log.info("REBALANCE revoking %s, committing processed offsets first", names)
        self.commit(partitions, reason="revoke")

    def on_lost(self, _consumer, partitions):
        # The member is already fenced. A commit here would be rejected.
        log.warning(
            "REBALANCE lost %s without a commit callback; in-flight records will be redelivered",
            [f"{p.topic}[{p.partition}]" for p in partitions],
        )

    def mark(self, msg):
        self.next_offset[(msg.topic(), msg.partition())] = msg.offset() + 1
        self.dirty = True

    def commit(self, partitions, reason: str):
        offsets = []
        for part in partitions:
            nxt = self.next_offset.get((part.topic, part.partition))
            if nxt is not None:
                offsets.append(TopicPartition(part.topic, part.partition, nxt))
        if not offsets:
            log.info("commit skipped (%s): nothing processed on %s", reason, partitions)
            return
        log.info(
            "commit %s offsets %s",
            reason,
            [f"{o.topic}[{o.partition}]@{o.offset}" for o in offsets],
        )
        self.consumer.commit(offsets=offsets, asynchronous=False)
        self.dirty = False
        self.last_commit = time.monotonic()

    def maybe_commit(self):
        if not self.dirty:
            return
        if (time.monotonic() - self.last_commit) * 1000 < COMMIT_INTERVAL_MS:
            return
        assignment = self.consumer.assignment()
        self.commit(assignment, reason="interval")

    def send_dlt(self, msg, reason: str):
        headers = [
            ("error-reason", reason.encode()),
            ("original-topic", msg.topic().encode()),
            ("original-partition", str(msg.partition()).encode()),
            ("original-offset", str(msg.offset()).encode()),
        ]
        done = {}

        def on_dlt(err, _report):
            done["err"] = err

        self.dlt.produce(
            DLT_TOPIC,
            key=msg.key(),
            value=msg.value(),
            headers=headers,
            on_delivery=on_dlt,
        )
        remaining = self.dlt.flush(30)
        if remaining or done.get("err") is not None:
            raise RuntimeError(f"dlt produce failed remaining={remaining} err={done.get('err')}")
        self.dlt_sent += 1
        log.error(
            "poison moved to %s user-event partition=%s offset=%s reason=%s",
            DLT_TOPIC,
            msg.partition(),
            msg.offset(),
            reason,
        )

    def handle(self, msg):
        last_error = "unknown"
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                event = json.loads(msg.value())
                if event.get("payload", {}).get("poison"):
                    raise RuntimeError("poison event")
                time.sleep(PROCESSING_DELAY_MS / 1000)
                inserted = self.sink.store(event, msg.topic(), msg.partition(), msg.offset())
                if not inserted:
                    self.duplicates += 1
                self.handled += 1
                self.mark(msg)
                return
            except Exception as exc:  # noqa: BLE001 - any processing failure counts as an attempt
                last_error = f"{type(exc).__name__}: {exc}"
                log.warning(
                    "attempt %s/%s failed partition=%s offset=%s error=%s",
                    attempt,
                    MAX_ATTEMPTS,
                    msg.partition(),
                    msg.offset(),
                    last_error,
                )
        self.send_dlt(msg, last_error)
        self.mark(msg)
        # Commit now so a crash does not run the poison event again.
        self.commit(
            [TopicPartition(msg.topic(), msg.partition())],
            reason="dlt",
        )


def main():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    log.info(
        "start consumer=%s group=%s delay_ms=%s commit_interval_ms=%s auto.offset.reset=earliest",
        CONSUMER_ID,
        GROUP_ID,
        PROCESSING_DELAY_MS,
        COMMIT_INTERVAL_MS,
    )
    sink = Sink(os.environ["DATABASE_URL"])
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
            "group.id": GROUP_ID,
            "client.id": f"consumer-{CONSUMER_ID}",
            "enable.auto.commit": False,
            # No committed offset yet (first start, or expired offsets): read the
            # backlog. "latest" would drop events produced before this member joined.
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky",
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000,
            "max.poll.interval.ms": 300000,
            "error_cb": lambda err: log.error("kafka client error: %s", err),
        }
    )
    dlt = Producer(
        {
            "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
            "client.id": f"consumer-{CONSUMER_ID}-dlt",
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "linger.ms": 0,
            "error_cb": lambda err: log.error("dlt client error: %s", err),
        }
    )
    processor = Processor(consumer, dlt, sink)
    consumer.subscribe(
        [TOPIC],
        on_assign=processor.on_assign,
        on_revoke=processor.on_revoke,
        on_lost=processor.on_lost,
    )

    last_stats = time.monotonic()
    try:
        while not stopping:
            msg = consumer.poll(1.0)
            if processor.log_resume:
                assigned = consumer.assignment()
                committed = consumer.committed(assigned, timeout=10)
                log.info(
                    "resume offsets %s (next record to read; -1001 means no commit yet, auto.offset.reset=earliest)",
                    [f"{part.topic}[{part.partition}]@{part.offset}" for part in committed],
                )
                processor.log_resume = False

            if msg is None:
                processor.maybe_commit()
            elif msg.error():
                log.error("poll error: %s", msg.error())
            else:
                processor.handle(msg)
                processor.maybe_commit()

            if time.monotonic() - last_stats > 10:
                log.info(
                    "stats handled=%s duplicates=%s dlt=%s",
                    processor.handled,
                    processor.duplicates,
                    processor.dlt_sent,
                )
                last_stats = time.monotonic()
    finally:
        assignment = consumer.assignment()
        if assignment:
            processor.commit(assignment, reason="shutdown")
        consumer.close()
        log.info(
            "stopped handled=%s duplicates=%s dlt=%s",
            processor.handled,
            processor.duplicates,
            processor.dlt_sent,
        )


if __name__ == "__main__":
    main()
