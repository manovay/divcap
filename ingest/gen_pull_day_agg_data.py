#!/usr/bin/env python3
"""
Sync day aggregates from Massive S3 into HDFS, month by month.

Runs ON THE CLUSTER (nyu-dataproc-m) -- it needs both the `massive` AWS
profile and the hdfs CLI. Shells out to `aws` and `hdfs` rather than using
boto3/hdfs libraries, because those two binaries are already configured and
working on this box.

Processes one month at a time and deletes the local copy immediately after
landing it in HDFS. Five years is ~400 MB total but only ~10 MB is ever on
local disk, which matters because /home is shared with the whole class.

Resumable: a month whose HDFS directory already holds the expected number of
files is skipped. Partial months are re-pulled, since `aws s3 sync` is itself
incremental and `hdfs dfs -put` is told to overwrite.

S3 objects are bare dates (2026-08-03.csv.gz); they are renamed to the
day_YYYY-MM-DD convention on the way in so one glob covers every directory.

WINDOW
Defaults to one month BEFORE the dividend history through the current month.
The extra month is not padding: an event with an ex-date in the first days of
2021-08 needs bars from late 2021-07 to fill its ex-5 window.

Run (from the cluster):
    python3 gen_pull_day_agg_data.py                    # 2021-07 .. current
    python3 gen_pull_day_agg_data.py 2024-01 2024-12
    python3 gen_pull_day_agg_data.py 2024-01 2024-12 minute_aggs_v1

This takes 20-40 minutes. Browser SSH sessions drop -- run it under tmux, or:
    nohup python3 gen_pull_day_agg_data.py > pull.log 2>&1 &
    tail -f pull.log

Needs the `massive` AWS profile in ~/.aws/credentials, $EP, and $TEAM.
"""

import os
import sys
import shutil
import datetime
import subprocess

TEAM = os.environ.get("TEAM", "/user/ms16965_nyu_edu/divcap")
EP = os.environ.get("EP", "https://files.massive.com")
PROFILE = "massive"
BUCKET = "s3://flatfiles/us_stocks_sip"

TMP = os.path.expanduser("~/pull_tmp")

# Dividend history starts 2021-08; pull one month earlier so the earliest
# events have a complete pre-window.
DEFAULT_START = "2021-07"


def sh(cmd, check=True):
    """Run a command, returning (rc, stdout)."""
    p = subprocess.run(cmd, capture_output=True, text=True)
    if check and p.returncode != 0:
        sys.exit(f"failed: {' '.join(cmd)}\n{p.stderr[:800]}")
    return p.returncode, p.stdout


def month_range(start, end):
    ys, ms = start.split("-")
    ye, me = end.split("-")
    y, m = int(ys), int(ms)
    ye, me = int(ye), int(me)
    out = []
    while (y, m) <= (ye, me):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def s3_count(dataset, ym):
    """How many objects S3 holds for this month. 0 means nothing to pull."""
    y, m = ym.split("-")
    rc, out = sh(["aws", "s3", "ls", f"{BUCKET}/{dataset}/{y}/{m}/",
                  "--endpoint-url", EP, "--profile", PROFILE], check=False)
    if rc != 0:
        return 0
    n = 0
    for line in out.splitlines():
        if line.strip().endswith(".csv.gz"):
            n += 1
    return n


def hdfs_count(hdfs_dir):
    """Files already landed. -1 if the directory does not exist."""
    rc, out = sh(["hdfs", "dfs", "-ls", hdfs_dir], check=False)
    if rc != 0:
        return -1
    n = 0
    for line in out.splitlines():
        if line.strip().endswith(".csv.gz"):
            n += 1
    return n


def pull_month(dataset, prefix, ym):
    """Returns (status, n_files)."""
    y, m = ym.split("-")
    hdfs_dir = f"{TEAM}/probe/{prefix}_{ym}"

    want = s3_count(dataset, ym)
    if want == 0:
        return "empty", 0

    have = hdfs_count(hdfs_dir)
    if have == want:
        return "skip", have

    local = os.path.join(TMP, ym)
    if os.path.exists(local):
        shutil.rmtree(local)
    os.makedirs(local)

    rc, _ = sh(["aws", "s3", "sync", f"{BUCKET}/{dataset}/{y}/{m}/", local,
                "--endpoint-url", EP, "--profile", PROFILE,
                "--only-show-errors"], check=False)

    # A 403 on GetObject with a working LIST means the month is outside the
    # plan's entitlement window, not a credential problem. Record it and keep
    # going -- one inaccessible month must not kill a 62-month run.
    files = []
    for f in sorted(os.listdir(local)):
        if f.endswith(".csv.gz"):
            files.append(f)
    if rc != 0 and not files:
        shutil.rmtree(local)
        return "FORBIDDEN", 0

    # S3 objects are bare dates; match the day_YYYY-MM-DD convention so one
    # glob covers every month directory.
    renamed = []
    for f in files:
        if f.startswith(f"{prefix}_"):
            renamed.append(f)
            continue
        new = f"{prefix}_{f}"
        os.rename(os.path.join(local, f), os.path.join(local, new))
        renamed.append(new)
    files = renamed

    if not files:
        shutil.rmtree(local)
        return "empty", 0

    sh(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])
    # subprocess does not expand globs, so pass explicit paths.
    paths = []
    for f in files:
        paths.append(os.path.join(local, f))
    sh(["hdfs", "dfs", "-put", "-f"] + paths + [hdfs_dir + "/"])

    landed = hdfs_count(hdfs_dir)
    shutil.rmtree(local)

    # A month straddling the entitlement boundary lands some files and 403s on
    # the rest. Those are worth keeping and worth flagging.
    if rc != 0:
        return "PARTIAL", landed
    if landed != want:
        return f"MISMATCH {landed}/{want}", landed
    return "ok", landed


def main():
    today = datetime.date.today()
    default_end = f"{today.year:04d}-{today.month:02d}"

    if len(sys.argv) > 1:
        start = sys.argv[1]
    else:
        start = DEFAULT_START

    if len(sys.argv) > 2:
        end = sys.argv[2]
    else:
        end = default_end

    if len(sys.argv) > 3:
        dataset = sys.argv[3]
    else:
        dataset = "day_aggs_v1"

    if dataset.startswith("minute"):
        prefix = "min"
    else:
        prefix = "day"

    months = month_range(start, end)
    os.makedirs(TMP, exist_ok=True)

    print(f"dataset: {dataset}  ->  {TEAM}/probe/{prefix}_<YYYY-MM>/")
    print(f"window:  {start} .. {end}  ({len(months)} months)\n")

    total = 0
    bad = []
    forbidden = []
    partial = []
    for ym in months:
        status, n = pull_month(dataset, prefix, ym)
        total += n
        print(f"  {ym}  {status:16s} {n:3d} files", flush=True)
        if status.startswith("MISMATCH"):
            bad.append(ym)
        elif status == "FORBIDDEN":
            forbidden.append(ym)
        elif status == "PARTIAL":
            partial.append(ym)

    shutil.rmtree(TMP, ignore_errors=True)

    print(f"\n{total} files across {len(months)} months")
    if forbidden:
        print(f"FORBIDDEN (outside the plan's entitlement window): {forbidden}")
    if partial:
        print(f"PARTIAL (some objects forbidden, rest landed): {partial}")
    if bad:
        print(f"MISMATCHED (re-run to retry): {bad}")
    if not (forbidden or partial or bad):
        print("all months landed cleanly")


if __name__ == "__main__":
    main()