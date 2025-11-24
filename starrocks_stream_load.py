import argparse
import os
import subprocess
import time
import json
import threading
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]

COLUMNS = {
    "customer": "c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment",
    "lineitem": "l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment",
    "nation": "n_nationkey,n_name,n_regionkey,n_comment",
    "orders": "o_orderkey,o_custkey,o_orderstatus,o_totalprice,o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment",
    "part": "p_partkey,p_name,p_mfgr,p_brand,p_type,p_size,p_container,p_retailprice,p_comment",
    "partsupp": "ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment",
    "region": "r_regionkey,r_name,r_comment",
    "supplier": "s_suppkey,s_name,s_address,s_nationkey,s_phone,s_acctbal,s_comment",
}

def find_files(table_dir: Path):
    files = []
    if table_dir.exists():
        for p in table_dir.iterdir():
            if p.is_file():
                files.append(p)
    return sorted(files)

def parse_result(proc: subprocess.CompletedProcess):
    ok = proc.returncode == 0
    out = (proc.stdout or "") + (proc.stderr or "")
    loaded = 0
    filtered = 0
    total = 0
    # Try JSON parse heuristics
    try:
        import json as _json
        jstart = out.find("{")
        if jstart != -1:
            j = _json.loads(out[jstart:])
            status = (j.get("Status") or j.get("status") or "")
            ok = ok and status.lower() == "success"
            loaded = int(j.get("NumberLoadedRows") or j.get("LoadedRows") or 0)
            filtered = int(j.get("NumberFilteredRows") or j.get("FilteredRows") or 0)
            total = int(j.get("NumberTotalRows") or j.get("TotalRows") or (loaded + filtered))
    except Exception:
        pass
    # regex fallback
    try:
        m = re.search(r"NumberLoadedRows\"?\s*:\s*(\d+)", out)
        if m:
            loaded = int(m.group(1))
        m = re.search(r"LoadedRows\"?\s*:\s*(\d+)", out)
        if m:
            loaded = int(m.group(1))
        m = re.search(r"NumberFilteredRows\"?\s*:\s*(\d+)", out)
        if m:
            filtered = int(m.group(1))
        m = re.search(r"FilteredRows\"?\s*:\s*(\d+)", out)
        if m:
            filtered = int(m.group(1))
        m = re.search(r"NumberTotalRows\"?\s*:\s*(\d+)", out)
        if m:
            total = int(m.group(1))
        m = re.search(r"TotalRows\"?\s*:\s*(\d+)", out)
        if m:
            total = int(m.group(1))
        sm = re.search(r"Status\"?\s*:\s*\"(\w+)\"", out)
        if sm:
            ok = ok and sm.group(1).lower() == "success"
    except Exception:
        pass
    return ok, out, loaded, filtered, total

def load_one(fe_host: str, fe_port: int, db: str, table: str, user: str, password: str, file_path: Path, timeout_s: int, sanitize: bool = True):
    url = f"http://{fe_host}:{fe_port}/api/{db}/{table}/_stream_load"
    base_cmd = [
        "curl",
        "--location-trusted",
        "-u",
        f"{user}:{password}",
        "-H",
        "Expect:100-continue",
        "-H",
        f"timeout:{timeout_s}",
        "-H",
        "max_filter_ratio:0.2",
        "-H",
        "column_separator:|",
        "-H",
        "strict_mode:false",
        "-H",
        "row_delimiter:\n",
        "-XPUT",
        url,
    ]
    cols = COLUMNS.get(table)
    if cols:
        base_cmd[12:12] = ["-H", f"columns:{cols}"]
    if sanitize:
        cmd = base_cmd + ["--data-binary", "@-"]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE, text=True)
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if line.endswith("|\n"):
                    line = line[:-2] + "\n"
                elif line.endswith("|"):
                    line = line[:-1]
                proc.stdin.write(line)
        try:
            proc.stdin.flush()
        except Exception:
            pass
        proc.stdin.close()
        out = proc.stdout.read() if proc.stdout else ""
        err = proc.stderr.read() if proc.stderr else ""
        ret = proc.wait()
        return subprocess.CompletedProcess(cmd, ret, out, err)
    else:
        cmd = base_cmd + ["-T", str(file_path)]
        return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config")
    p.add_argument("--fe-host")
    p.add_argument("--fe-port", type=int)
    p.add_argument("--db")
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--data-dir")
    p.add_argument("--concurrency", type=int)
    p.add_argument("--timeout", type=int)
    p.add_argument("--progress", action="store_true")
    p.add_argument("--sanitize", action="store_true")
    args = p.parse_args()
    cfg = {}
    if args.config:
        cfg_path = Path(args.config)
        if not cfg_path.exists():
            raise SystemExit("config file not found")
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    fe_host = args.fe_host or cfg.get("fe_host") or cfg.get("fe_host_name")
    fe_port = args.fe_port or cfg.get("fe_http_port") or cfg.get("fe_port")
    db = args.db or cfg.get("db") or cfg.get("database")
    user = args.user or cfg.get("user") or cfg.get("username")
    password = args.password or cfg.get("password")
    data_dir = args.data_dir or cfg.get("data_dir")
    concurrency = args.concurrency or cfg.get("concurrency", 10)
    timeout = args.timeout or cfg.get("timeout", 3600)
    progress = args.progress or bool(cfg.get("progress", True))
    sanitize = args.sanitize or bool(cfg.get("sanitize", True))
    if not all([fe_host, fe_port, db, user, password, data_dir]):
        raise SystemExit("missing required settings: fe_host, fe_port, db, user, password, data_dir")
    root = Path(data_dir).absolute()
    jobs = []
    for t in TABLES:
        tdir = root / t
        for f in find_files(tdir):
            jobs.append((t, f))
    totals = {}
    for t, _ in jobs:
        totals[t] = totals.get(t, 0) + 1
    status = {t: {"done": 0, "success": 0, "fail": 0, "total": totals.get(t, 0), "loaded_rows": 0, "filtered_rows": 0} for t in totals}
    lock = threading.Lock()
    results = []
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = {}
        for t, f in jobs:
            fut = ex.submit(load_one, fe_host, int(fe_port), db, t, user, password, f, int(timeout), sanitize)
            futs[ fut ] = (t, f)
        for fu in as_completed(list(futs.keys())):
            t, f = futs[fu]
            proc = fu.result()
            ok, out, loaded_rows, filtered_rows, total_rows = parse_result(proc)
            with lock:
                st = status[t]
                st["done"] += 1
                if ok:
                    st["success"] += 1
                else:
                    st["fail"] += 1
                st["loaded_rows"] += loaded_rows
                st["filtered_rows"] += filtered_rows
                if progress:
                    total = st["total"]
                    done = st["done"]
                    pct = int((done / total) * 100) if total else 100
                    bars = int((done / total) * 20) if total else 20
                    bar = "#" * bars + "." * (20 - bars)
                    print(f"{t}: [{bar}] {done}/{total} {pct}% success={st['success']} fail={st['fail']} loaded_rows={st['loaded_rows']} filtered_rows={st['filtered_rows']}")
            results.append(proc)
    failed = [r for r in results if r.returncode != 0]
    if failed:
        print("\n=== Failed Responses ===")
        for r in failed:
            print((r.stdout or "") + (r.stderr or ""))
    # Print summary per table
    if progress:
        print("\n=== Load Summary ===")
        for t in status:
            st = status[t]
            print(f"{t}: files={st['total']} success={st['success']} fail={st['fail']} loaded_rows={st['loaded_rows']} filtered_rows={st['filtered_rows']}")
    
    if failed:
        raise SystemExit(1)

if __name__ == "__main__":
    main()

