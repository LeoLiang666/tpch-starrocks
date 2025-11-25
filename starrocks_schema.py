import argparse
import json
from pathlib import Path

BASE_BUCKETS = {
    "customer": 24,
    "lineitem": 96,
    "orders": 96,
    "part": 24,
    "partsupp": 24,
    "supplier": 12,
    "nation": 1,
    "region": 1,
}

SCALE = {
    "10GB": 1,
    "100GB": 1,
    "1TB": 10,
    "10TB": 100,
    "100TB": 1000,
}

def buckets(table: str, size: str) -> int:
    base = BASE_BUCKETS[table]
    factor = SCALE.get(size, 1)
    if table in {"customer", "lineitem", "orders", "part", "partsupp", "supplier"}:
        return max(1, base * factor)
    return base

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config")
    p.add_argument("--db")
    p.add_argument("--size", choices=list(SCALE.keys()))
    args = p.parse_args()
    cfg = {}
    if args.config:
        cfg_path = Path(args.config)
        if not cfg_path.exists():
            raise SystemExit("config file not found")
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    db = args.db or cfg.get("db") or cfg.get("database")
    size = args.size or cfg.get("size") or cfg.get("schema_size") or "100GB"
    if not db:
        raise SystemExit("missing database name")
    print(f"CREATE DATABASE IF NOT EXISTS `{db}`;")
    print(f"USE `{db}`;")
    print("drop table if exists customer;")
    print(f"CREATE TABLE customer (\n    c_custkey     int NOT NULL,\n    c_name        VARCHAR(25) NOT NULL,\n    c_address     VARCHAR(40) NOT NULL,\n    c_nationkey   int NOT NULL,\n    c_phone       VARCHAR(15) NOT NULL,\n    c_acctbal     decimal(15, 2)   NOT NULL,\n    c_mktsegment  VARCHAR(10) NOT NULL,\n    c_comment     VARCHAR(117) NOT NULL\n)ENGINE=OLAP\nDUPLICATE KEY(`c_custkey`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`c_custkey`) BUCKETS {buckets('customer', size)}\nPROPERTIES (\n    \"replication_num\" = \"3\"\n);")
    print("drop table if exists lineitem;")
    print(f"CREATE TABLE lineitem (\n    l_shipdate    DATE NOT NULL,\n    l_orderkey    int NOT NULL,\n    l_linenumber  int not null,\n    l_partkey     int NOT NULL,\n    l_suppkey     int not null,\n    l_quantity    decimal(15, 2) NOT NULL,\n    l_extendedprice  decimal(15, 2) NOT NULL,\n    l_discount    decimal(15, 2) NOT NULL,\n    l_tax         decimal(15, 2) NOT NULL,\n    l_returnflag  VARCHAR(1) NOT NULL,\n    l_linestatus  VARCHAR(1) NOT NULL,\n    l_commitdate  DATE NOT NULL,\n    l_receiptdate DATE NOT NULL,\n    l_shipinstruct VARCHAR(25) NOT NULL,\n    l_shipmode     VARCHAR(10) NOT NULL,\n    l_comment      VARCHAR(44) NOT NULL\n)ENGINE=OLAP\nDUPLICATE KEY(`l_shipdate`, `l_orderkey`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`l_orderkey`) BUCKETS {buckets('lineitem', size)}\nPROPERTIES (\n    \"replication_num\" = \"3\",\n    \"colocate_with\" = \"tpch2\"\n);")
    print("drop table if exists nation;")
    print(f"CREATE TABLE nation (\n  n_nationkey int NOT NULL,\n  n_name      varchar(25) NOT NULL,\n  n_regionkey int NOT NULL,\n  n_comment   varchar(152) NULL\n) ENGINE=OLAP\nDUPLICATE KEY(`n_nationkey`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`n_nationkey`) BUCKETS {buckets('nation', size)}\nPROPERTIES (\n    \"replication_num\" = \"3\"\n);")
    print("drop table if exists orders;")
    print(f"CREATE TABLE orders  (\n    o_orderkey       int NOT NULL,\n    o_orderdate      DATE NOT NULL,\n    o_custkey        int NOT NULL,\n    o_orderstatus    VARCHAR(1) NOT NULL,\n    o_totalprice     decimal(15, 2) NOT NULL,\n    o_orderpriority  VARCHAR(15) NOT NULL,\n    o_clerk          VARCHAR(15) NOT NULL,\n    o_shippriority   int NOT NULL,\n    o_comment        VARCHAR(79) NOT NULL\n)ENGINE=OLAP\nDUPLICATE KEY(`o_orderkey`, `o_orderdate`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`o_orderkey`) BUCKETS {buckets('orders', size)}\nPROPERTIES (\n    \"replication_num\" = \"3\",\n    \"colocate_with\" = \"tpch2\"\n);")
    print("drop table if exists part;")
    print(f"CREATE TABLE part (\n    p_partkey          int NOT NULL,\n    p_name        VARCHAR(55) NOT NULL,\n    p_mfgr        VARCHAR(25) NOT NULL,\n    p_brand       VARCHAR(10) NOT NULL,\n    p_type        VARCHAR(25) NOT NULL,\n    p_size        int NOT NULL,\n    p_container   VARCHAR(10) NOT NULL,\n    p_retailprice decimal(15, 2) NOT NULL,\n    p_comment     VARCHAR(23) NOT NULL\n)ENGINE=OLAP\nDUPLICATE KEY(`p_partkey`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`p_partkey`) BUCKETS {buckets('part', size)}\nPROPERTIES (\n    \"replication_num\" = \"3\",\n    \"colocate_with\" = \"tpch2p\"\n);")
    print("drop table if exists partsupp;")
    print(f"CREATE TABLE partsupp (\n    ps_partkey          int NOT NULL,\n    ps_suppkey     int NOT NULL,\n    ps_availqty    int NOT NULL,\n    ps_supplycost  decimal(15, 2)  NOT NULL,\n    ps_comment     VARCHAR(199) NOT NULL\n)ENGINE=OLAP\nDUPLICATE KEY(`ps_partkey`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`ps_partkey`) BUCKETS {buckets('partsupp', size)}\nPROPERTIES (\n    \"replication_num\" = \"3\",\n    \"colocate_with\" = \"tpch2p\"\n);")
    print("drop table if exists region;")
    print(f"CREATE TABLE region  (\n    r_regionkey      int NOT NULL,\n    r_name       VARCHAR(25) NOT NULL,\n    r_comment    VARCHAR(152)\n)ENGINE=OLAP\nDUPLICATE KEY(`r_regionkey`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`r_regionkey`) BUCKETS {buckets('region', size)}\nPROPERTIES (\n    \"replication_num\" = \"3\"\n);")
    print("drop table if exists supplier;")
    print(f"CREATE TABLE supplier (\n    s_suppkey       int NOT NULL,\n    s_name        VARCHAR(25) NOT NULL,\n    s_address     VARCHAR(40) NOT NULL,\n    s_nationkey   int NOT NULL,\n    s_phone       VARCHAR(15) NOT NULL,\n    s_acctbal     decimal(15, 2) NOT NULL,\n    s_comment     VARCHAR(101) NOT NULL\n)ENGINE=OLAP\nDUPLICATE KEY(`s_suppkey`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`s_suppkey`) BUCKETS {buckets('supplier', size)}\nPROPERTIES (\n    \"replication_num\" = \"3\"\n);")

if __name__ == "__main__":
    main()

