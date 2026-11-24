"""Check the sink and events.dlt.

A seq missing from the sink is not a loss when that event is in events.dlt.
The first line is the GitHub login and the current time, for screenshots.
"""

import json
import os
import subprocess
import sys
from collections import defaultdict

import psycopg
from confluent_kafka import Consumer, TopicPartition


def identity_line() -> str:
    login = os.environ.get("GITHUB_LOGIN", "juzver25")
    current = subprocess.check_output(["date"], text=True).strip()
    return f"{login} {current}"


def count_inversions(seqs_in_time_order: list[int]) -> int:
    """Pairs where a smaller seq was stored later than a larger seq."""

    def sort_count(items: list[int]) -> tuple[list[int], int]:
        if len(items) <= 1:
            return items, 0
        mid = len(items) // 2
        left, left_inv = sort_count(items[:mid])
        right, right_inv = sort_count(items[mid:])
        merged: list[int] = []
        i = j = 0
        inversions = left_inv + right_inv
        while i < len(left) and j < len(right):
            if left[i] <= right[j]:
                merged.append(left[i])
                i += 1
            else:
                merged.append(right[j])
                inversions += len(left) - i
                j += 1
        merged.extend(left[i:])
        merged.extend(right[j:])
        return merged, inversions

    _, inversions = sort_count(seqs_in_time_order)
    return inversions


def read_dlt(bootstrap: str) -> dict[str, dict]:
    """event_id -> event. Duplicate DLT copies of the same event_id collapse."""
    topic = os.environ.get("DLT_TOPIC", "events.dlt")
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "verify-readonly",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    found: dict[str, dict] = {}
    try:
        meta = consumer.list_topics(topic, timeout=15)
        topic_meta = meta.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None:
            raise RuntimeError(f"topic {topic} is not available: {topic_meta}")
        ends: dict[int, int] = {}
        assignment = []
        for partition_id in sorted(topic_meta.partitions):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition_id), timeout=15
            )
            ends[partition_id] = high
            assignment.append(TopicPartition(topic, partition_id, low))
        if all(high == 0 for high in ends.values()):
            return found
        consumer.assign(assignment)
        idle_polls = 0
        while True:
            if all(consumer.position([TopicPartition(topic, pid)])[0].offset >= high for pid, high in ends.items()):
                break
            msg = consumer.poll(1.0)
            if msg is None:
                idle_polls += 1
                if idle_polls > 15:
                    break
                continue
            idle_polls = 0
            if msg.error():
                print(f"dlt poll error: {msg.error()}", file=sys.stderr)
                continue
            event = json.loads(msg.value())
            found[event["event_id"]] = event
    finally:
        consumer.close()
    return found


def read_sink(dsn: str):
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, seq, event_id, processed_at
                FROM processed_events
                ORDER BY user_id, processed_at, seq
                """
            )
            rows = cur.fetchall()
            cur.execute("SELECT dropped_duplicates FROM dedup_stats WHERE id = 1")
            dropped = cur.fetchone()[0]
    return rows, dropped


def main():
    # Printed before any I/O so a screenshot of a failure still has the identity line.
    print(identity_line(), flush=True)

    rows, dropped = read_sink(os.environ["DATABASE_URL"])
    dlt = read_dlt(os.environ["BOOTSTRAP_SERVERS"])

    by_user_sink: dict[str, list[tuple[int, str]]] = defaultdict(list)
    sink_ids = set()
    for user_id, seq, event_id, _processed_at in rows:
        by_user_sink[user_id].append((seq, event_id))
        sink_ids.add(event_id)

    dlt_by_user: dict[str, set[int]] = defaultdict(set)
    for event in dlt.values():
        dlt_by_user[event["user_id"]].add(event["seq"])

    users = sorted(set(by_user_sink) | set(dlt_by_user))
    gaps = []
    order_violations = 0
    violations_by_user = []
    for user_id in users:
        sink_seqs = [seq for seq, _event_id in by_user_sink.get(user_id, [])]
        present = set(sink_seqs) | dlt_by_user.get(user_id, set())
        if not present:
            continue
        missing = [seq for seq in range(1, max(present) + 1) if seq not in present]
        if missing:
            gaps.append((user_id, missing))
        # processed_at order is the SELECT order.
        user_inversions = count_inversions(sink_seqs)
        order_violations += user_inversions
        if user_inversions:
            violations_by_user.append((user_id, user_inversions))

    print(f"пользователей: {len(users)}")
    print(f"событий в sink: {len(rows)}")
    print(f"уникальных событий в events.dlt: {len(dlt)}")
    print(f"пропусков seq: {sum(len(missing) for _user, missing in gaps)}")
    print(f"отброшено дублей: {dropped}")
    print(f"нарушений порядка: {order_violations}")
    if violations_by_user:
        print("нарушения по пользователям:")
        for user_id, count in sorted(violations_by_user, key=lambda item: -item[1])[:12]:
            print(f"  {user_id}: {count}")
    if gaps:
        print("пропуски:")
        for user_id, missing in gaps:
            shown = missing[:20]
            suffix = "" if len(missing) <= 20 else f" ... ещё {len(missing) - 20}"
            print(f"  {user_id}: {shown}{suffix}")
    else:
        print("пропусков нет: каждый seq есть в sink или в events.dlt")

    poison_in_sink = sink_ids & set(dlt)
    print(f"event_id одновременно в sink и в dlt: {len(poison_in_sink)}")
    return 0 if not gaps else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
