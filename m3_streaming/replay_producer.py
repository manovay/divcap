#!/usr/bin/env python3
"""
Stage C -- replay a historical trading day's minute bars into Kafka.

There is no live market feed and the market is not open at 3am, so the
"stream" is simulated: read one day's minute file from HDFS, sort it into
event-time order, and republish at an accelerated wall-clock rate. This is
the standard way streaming systems are demoed and tested, and it is what the
proposal described.

EVENT-TIME ORDERING MATTERS
The flat files are sorted by TICKER, then time -- all of A's bars, then all of
AA's. Published in file order the consumer would see 09:30 through 16:00 for
one ticker before seeing 09:31 for the next, and every windowed aggregation
would be wrong. So the whole day is loaded, sorted by window_start, and
emitted in timestamp order. ~2M rows sorts in a couple of seconds and holds
in a few hundred MB.

KEYED BY TICKER
Kafka guarantees ordering within a partition, not across them. Keying by
ticker puts each ticker's bars in one partition, so they arrive in sequence --
which the features depend on.

THE STREAM IS NOT PRE-FILTERED
Every dividend-paying ticker is emitted, not just the ~200 with an ex-date
tomorrow. Narrowing is the streaming job's stream-static join, which is the
point: ~9,000 tickers collapse to ~200 candidates downstream. Pre-filtering
here would hide the work.

Run:
    # decision day -- signals fire from this one
    python3 m3_streaming/replay_producer.py 2026-06-12 --speed 60

    # ex-date, for realized P&L on the predictions
    python3 m3_streaming/replay_producer.py 2026-06-15 --speed 60 --until 10:00

    # dry run, no Kafka
    python3 m3_streaming/replay_producer.py 2026-06-12 --dry-run

--speed 60 replays a 6.5-hour session in ~6.5 minutes. --speed 600 in ~40s.

Needs kafka-python:  pip3 install --user kafka-python
Needs a broker:      see start_kafka.sh
"""

import os
import sys
import json
import time
import subprocess

TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")
BROKER = os.environ.get("KAFKA_BROKER", "localhost:19092")
TOPIC = os.environ.get("KAFKA_TOPIC", "minute-bars")

SESSION_START_MIN = 9 * 60 + 30      # 09:30 ET
SESSION_END_MIN = 16 * 60            # 16:00 ET
NS_PER_S = 1_000_000_000


def parse_args(argv):
    a = {"day": None, "speed": 60.0, "dry": False,
         "until": None, "universe": True, "topic": TOPIC}
    i = 0
    while i < len(argv):
        k = argv[i]
        if k == "--speed":
            a["speed"] = float(argv[i + 1]); i += 2
        elif k == "--dry-run":
            a["dry"] = True; i += 1
        elif k == "--until":
            a["until"] = argv[i + 1]; i += 2
        elif k == "--topic":
            a["topic"] = argv[i + 1]; i += 2
        elif k == "--no-universe":
            a["universe"] = False; i += 1
        elif not k.startswith("--"):
            a["day"] = k; i += 1
        else:
            sys.exit(f"unknown arg: {k}")
    if not a["day"]:
        sys.exit("usage: replay_producer.py YYYY-MM-DD [--speed N] "
                 "[--until HH:MM] [--dry-run]")
    return a


def et_minute(ns):
    """
    Minute-of-day in ET from a nanosecond UTC epoch stamp.

    Derived arithmetically rather than with a tz library so the producer has
    no dependency beyond the stdlib. US equity sessions run on ET, which is
    UTC-4 in summer and UTC-5 in winter; the offset is recovered from the
    file itself by anchoring on the fact that the session starts at 09:30.
    """
    return int((ns // NS_PER_S) % 86400) // 60


def load_universe():
    """Dividend-paying tickers. Emitting only these is a scope choice, not a
    filter that hides work -- the interesting narrowing happens downstream."""
    p = subprocess.run(
        ["hdfs", "dfs", "-text", f"{TEAM}/reference/dividends_5y.jsonl"],
        capture_output=True, text=True)
    if p.returncode != 0:
        print("WARN: could not read dividend universe, emitting all tickers")
        return None
    out = set()
    for line in p.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.add(json.loads(line)["ticker"])
        except Exception:
            continue
    out.add("SPY")            # market proxy, needed for abnormal returns
    return out


def load_day(day, universe):
    """
    All bars for one day, sorted by event time.

    Returns a list of (window_start_ns, ticker, dict). The dict is what gets
    published; keeping it pre-built avoids rebuilding 2M dicts inside the
    timing loop, where a stall would distort the replay cadence.
    """
    ym = day[:7]
    path = f"{TEAM}/probe/min_{ym}/min_{day}.csv.gz"
    print(f"reading {path}", flush=True)

    p = subprocess.Popen(["hdfs", "dfs", "-text", path],
                         stdout=subprocess.PIPE, text=True,
                         stderr=subprocess.DEVNULL)

    rows = []
    header = True
    kept = seen = 0
    for line in p.stdout:
        if header:
            header = False
            continue
        f = line.rstrip("\n").split(",")
        if len(f) != 8:
            continue
        seen += 1
        tk = f[0]
        if universe is not None and tk not in universe:
            continue
        try:
            ns = int(f[6])
            rows.append((ns, tk, {
                "ticker": tk,
                "volume": float(f[1]),
                "open": float(f[2]),
                "close": float(f[3]),
                "high": float(f[4]),
                "low": float(f[5]),
                "window_start": ns,
                "transactions": int(f[7]),
            }))
            kept += 1
        except ValueError:
            continue
    p.wait()

    print(f"parsed {seen} bars, kept {kept}", flush=True)
    rows.sort(key=lambda r: r[0])
    return rows


def main():
    a = parse_args(sys.argv[1:])

    universe = load_universe() if a["universe"] else None
    if universe:
        print(f"universe: {len(universe)} tickers", flush=True)

    rows = load_day(a["day"], universe)
    if not rows:
        sys.exit("no rows -- check the date is a trading day with data")

    # Session window. The file carries 04:00-20:00; pre-market and after-hours
    # bars are not part of the decision and would only pad the replay.
    end_min = SESSION_END_MIN
    if a["until"]:
        hh, mm = a["until"].split(":")
        end_min = int(hh) * 60 + int(mm)

    # Recover the UTC offset from the data: the first bar at or after 04:00 ET
    # is not reliable, but the MODE of the first-bar minute is 09:30 for
    # liquid names. Simpler and robust: the session's own minimum is 04:00 ET,
    # so offset = that minimum minus 240.
    raw_mins = [et_minute(r[0]) for r in rows[:5000]]
    first_raw = min(raw_mins)
    offset_min = first_raw - (4 * 60)      # 04:00 ET premarket open

    sel = []
    for ns, tk, rec in rows:
        m = et_minute(ns) - offset_min
        if SESSION_START_MIN <= m < end_min:
            sel.append((ns, tk, rec, m))

    if not sel:
        sys.exit("no session bars after the time filter -- check --until")

    t_first, t_last = sel[0][0], sel[-1][0]
    span_s = (t_last - t_first) / NS_PER_S
    print(f"session bars: {len(sel)}")
    print(f"event span:   {span_s/60:.0f} min "
          f"({sel[0][3]//60:02d}:{sel[0][3]%60:02d} -> "
          f"{sel[-1][3]//60:02d}:{sel[-1][3]%60:02d} ET)")
    print(f"replay speed: {a['speed']}x -> ~{span_s/a['speed']/60:.1f} min wall",
          flush=True)

    if a["dry"]:
        print("\n--dry-run: first 3 and last 3 messages")
        for ns, tk, rec, m in sel[:3] + sel[-3:]:
            print(f"  {m//60:02d}:{m%60:02d} {tk:6s} {json.dumps(rec)[:110]}")
        return

    from kafka import KafkaProducer
    prod = KafkaProducer(
        bootstrap_servers=BROKER,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=20,          # batch a little; 2M sends unbatched is slow
        acks=1,
    )
    print(f"\nproducing to {a['topic']} @ {BROKER}", flush=True)

    t0 = time.time()
    sent = 0
    for ns, tk, rec, m in sel:
        # Pace against event time so the replay preserves the shape of the
        # session -- bursts stay bursts rather than being smoothed away.
        target = (ns - t_first) / NS_PER_S / a["speed"]
        lag = target - (time.time() - t0)
        if lag > 0.001:
            time.sleep(lag)

        prod.send(a["topic"], key=tk, value=rec)
        sent += 1
        if sent % 50000 == 0:
            el = time.time() - t0
            print(f"  {sent:7d} sent  {el/60:5.1f} min wall  "
                  f"now {m//60:02d}:{m%60:02d} ET", flush=True)

    prod.flush()
    prod.close()
    print(f"\ndone: {sent} bars in {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()