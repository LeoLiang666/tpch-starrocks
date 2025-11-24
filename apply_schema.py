import argparse
import json
import subprocess
from pathlib import Path

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--file", default="schema.sql")
    args = p.parse_args()
    cfg_path = Path(args.config)
    if not cfg_path.exists():
        raise SystemExit("config file not found")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    host = cfg.get("fe_host") or cfg.get("fe_host_name")
    port = int(cfg.get("fe_query_port") or cfg.get("fe_port") or 9030)
    user = cfg.get("user") or cfg.get("username")
    password = cfg.get("password") or ""
    sql_file = Path(args.file)
    if not sql_file.exists():
        raise SystemExit("schema file not found")
    cmd = [
        "mysql",
        "-h",
        str(host),
        "-P",
        str(port),
        "-u",
        str(user),
        "-p" + str(password),
    ]
    with open(sql_file, "rb") as fin:
        r = subprocess.run(cmd, stdin=fin)
        if r.returncode != 0:
            raise SystemExit(r.returncode)

if __name__ == "__main__":
    main()

