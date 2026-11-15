#!/usr/bin/env python3
"""Ingest retry-storm delivery batches into events.db.

Batch file block format (a delivery / replica):

    =<delivery_id> <tid> <seq>
    at <event timestamp>
    payload <N>
    pl <payload line 1>
    ...
    pl <payload line N>
    end =<delivery_id>

Retries redeliver the same business event under a new delivery id; all other
fields are identical. A replica cut off mid-delivery loses its tail (some pl
lines and the end marker); the surviving lines are a prefix of a complete
replica. Therefore:

* event identity = business key (tid, seq), delivery ids are not identity;
* the complete replica is authoritative (truncated lines add no information,
  they are only a prefix, so replicas must never be spliced together);
* an event whose every replica is truncated cannot be entered as a complete
  event and is skipped;
* ingestion is idempotent: replaying a batch after partial success hits the
  PRIMARY KEY conflict and never duplicates an already-entered event.
"""

import argparse
import glob
import json
import os
import re
import sqlite3
import sys

HEADER_RE = re.compile(r"=(d\d+)\s+(\S+)\s+(\d+)\s*$")
END_RE = re.compile(r"end\s+=(d\d+)\s*$")


def parse_batch(path):
    """Parse one batch file into replica dicts.

    Each replica: dict(delivery_id, tid, seq, at, declared_n, payload(list),
    complete(bool), source(batch file name)).
    """
    replicas = []
    with open(path, "r", encoding="utf-8") as fh:
        lines = fh.read().splitlines()

    i = 0
    n_lines = len(lines)
    while i < n_lines:
        line = lines[i]
        m = HEADER_RE.match(line)
        if not m:
            # Anything outside a block (blank lines etc.) is ignored.
            i += 1
            continue

        delivery_id, tid, seq = m.groups()
        block_start = i
        i += 1

        at = None
        if i < n_lines and lines[i].startswith("at "):
            at = lines[i][3:]
            i += 1

        declared_n = 0
        if i < n_lines and lines[i].startswith("payload "):
            declared_n = int(lines[i][len("payload "):])
            i += 1

        payload = []
        while i < n_lines and lines[i].startswith("pl "):
            payload.append(lines[i][3:])
            i += 1

        end_id = None
        if i < n_lines:
            em = END_RE.match(lines[i])
            if em:
                end_id = em.group(1)
                i += 1
        # If no end marker is present here, the next iteration lands on the
        # next header (or EOF): this replica was truncated mid-delivery.

        complete = (
            end_id == delivery_id
            and at is not None
            and len(payload) == declared_n
        )
        replicas.append({
            "delivery_id": delivery_id,
            "tid": tid,
            "seq": int(seq),
            "at": at,
            "declared_n": declared_n,
            "payload": payload,
            "complete": complete,
            "source": os.path.basename(path),
        })

    return replicas


def pick_best(replicas):
    """Choose the authoritative replica for one event.

    Formation rule: truncation only removes tail lines from an identical
    redelivery, so the longest complete replica wins. Complete replicas carry
    identical payloads, so the choice is deterministic in content.
    """
    return max(
        replicas,
        key=lambda r: (r["complete"], len(r["payload"]), r["delivery_id"]),
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="/workspace/events.db",
                    help="sqlite database path")
    ap.add_argument("--stats", default="/workspace/dup_stats.txt",
                    help="dedup statistics output path")
    ap.add_argument("batches", nargs="*",
                    help="batch files (defaults to events/batch_*.txt)")
    args = ap.parse_args()

    batch_paths = args.batches or sorted(
        glob.glob("/workspace/events/batch_*.txt"))
    if not batch_paths:
        sys.exit("no batch files found")

    replicas = []
    for path in batch_paths:
        replicas.extend(parse_batch(path))

    # Group replicas by business identity.
    groups = {}
    first_seen = {}
    for order, r in enumerate(replicas):
        key = (r["tid"], r["seq"])
        groups.setdefault(key, []).append(r)
        first_seen.setdefault(key, order)

    event_count = len(groups)
    replica_count = len(replicas)
    duplicate_count = replica_count - event_count
    truncated_count = sum(1 for r in replicas if not r["complete"])

    conn = sqlite3.connect(args.db)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                tid     TEXT    NOT NULL,
                seq     INTEGER NOT NULL,
                at      TEXT    NOT NULL,
                payload TEXT    NOT NULL,
                PRIMARY KEY (tid, seq)
            ) WITHOUT ROWID
            """
        )

        inserted = 0
        replayed = 0          # complete events already present on replay
        incomplete = []       # events with no complete replica
        for key, peers in groups.items():
            best = pick_best(peers)
            if not best["complete"]:
                incomplete.append(key)
                continue
            cur = conn.execute(
                "INSERT OR IGNORE INTO events (tid, seq, at, payload) "
                "VALUES (?, ?, ?, ?)",
                (best["tid"], best["seq"], best["at"],
                 json.dumps(best["payload"], ensure_ascii=False)),
            )
            if cur.rowcount == 1:
                inserted += 1
            else:
                replayed += 1
        conn.commit()

        ingested = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    finally:
        conn.close()

    should_deliver = event_count
    completion_rate = ingested / should_deliver * 100.0

    with open(args.stats, "w", encoding="utf-8") as fh:
        fh.write(f"事件数: {event_count}\n")
        fh.write(f"副本数: {replica_count}\n")
        fh.write(f"去重数: {duplicate_count}\n")
        fh.write(f"截断副本数: {truncated_count}\n")
        fh.write(f"入账事件数: {ingested}\n")
        fh.write(f"应投事件数: {should_deliver}\n")
        fh.write(f"入账完成率: {completion_rate:.1f}%\n")

    print(f"batches scanned      : {len(batch_paths)}")
    print(f"replicas parsed      : {replica_count}")
    print(f"distinct events      : {event_count}")
    print(f"duplicates collapsed : {duplicate_count}")
    print(f"truncated replicas   : {truncated_count}")
    print(f"inserted this run    : {inserted}")
    print(f"already present      : {replayed} (replayed, not duplicated)")
    print(f"skipped (no complete replica): {len(incomplete)}")
    for tid, seq in sorted(incomplete):
        print(f"  - {tid} seq {seq}")
    print(f"rows in events.db    : {ingested}")
    print(f"completion rate      : {completion_rate:.1f}%")
    print(f"stats written        : {args.stats}")


if __name__ == "__main__":
    main()
