# TPCH 数据生成与 StarRocks 导入使用手册

## 概述
- 提供一个简洁高效的 Python 工具链，在 Linux 上按 TPC-H 官方规则生成数据，并以 Stream Load 并发导入 StarRocks。
- 组成：
  - `tpch_gen.py`：并行生成 TPC-H 文本数据，单文件最大 5GB，按表归档。
  - `starrocks_schema.py`：根据规模自动调整大表 BUCKETS，输出建库建表 SQL。
  - `starrocks_stream_load.py`：并发 Stream Load 导入各表所有分片。

## 目录结构
- 生成后输出形如：
  - `<out>/<SIZE>/<table>/<table>.tbl.<stream_id>` 或切分为 `*.part-00000.tbl`
  - 例如：`/home/disk1/liangchaohua/tpch/data/10GB/lineitem/lineitem.tbl.1`
- 列分隔符为 `|`，与 StarRocks 加载参数 `column_separator:|` 对齐。

## 环境要求
- Linux（Python 3.8+）
- 构建工具：`make`、`gcc`
- 下载工具（HTTPS 不可用时回退）：`wget` 或 `curl`

## 安装依赖
```bash
# Debian/Ubuntu
sudo apt-get update
sudo apt-get install -y python3 python3-pip make gcc wget curl

# CentOS/RHEL
sudo yum install -y python3 python3-pip make gcc wget curl
```

## 生成数据
- 示例：生成 10GB 数据（16 线程）
```bash
python3 tpch_gen.py \
  --size 10GB \
  --outdir /home/disk1/liangchaohua/tpch/data \
  --threads 16
```
- 规模映射（TPC-H SF）：
  - `10GB→SF=10`，`100GB→100`，`1TB→1000`，`10TB→10000`，`100TB→100000`
- 可选参数：
  - `--max-file-size-gb` 单文件最大值（默认 5）
  - `--dbgen` 指定已编译好的 `dbgen` 路径（跳过下载与编译）
  - `--chunks` 强制指定并行流数（通常无需手动设置）
- 首次运行会自动下载并编译 `dbgen`；若 HTTPS 不可用会回退到 `wget`/`curl`。
### 生成阶段清理与小表搬运
- 自动去除每行末尾多余的 `|`，避免导入时列数不匹配。
- 并行模式下 `nation`、`region` 可能生成不带流后缀的 `*.tbl` 文件，已自动搬运并重命名为 `*.tbl.1`。

## BUCKETS 规则与建表 SQL
- 大表按规模倍增：`customer`、`lineitem`、`orders`、`part`、`partsupp`
  - `100GB→×1`、`1TB→×10`、`10TB→×100`、`100TB→×1000`
- 小表固定：`nation`、`region`、`supplier` 不翻倍。
- 所有建表 `replication_num` 统一为 `3`。
- 生成建表 SQL：
```bash
python3 starrocks_schema.py --db tpch_db --size 100GB > schema.sql
mysql -h <fe_host> -P <fe_query_port> -u <user> -p -D tpch_db < schema.sql
```

### 使用配置文件执行建表
- 在 `starrocks_config.json` 中新增 `fe_query_port`，可通过脚本直接执行：
```bash
python3 starrocks_schema.py --config starrocks_config.json > schema.sql
python3 apply_schema.py --config starrocks_config.json --file schema.sql
```

## 配置文件
- 支持通过 JSON 配置集中管理 FE/DB/账号/并发等参数，命令行参数可覆盖配置。
- 示例文件：`starrocks_config.json`
```json
{
  "fe_host": "127.0.0.1",
  "fe_http_port": 8030,
  "db": "tpch_db",
  "user": "admin",
  "password": "your_password",
  "concurrency": 10,
  "timeout": 3600,
  "data_dir": "/home/disk1/liangchaohua/tpch/data/10GB",
  "schema_size": "100GB"
}
```
- 使用配置生成建表 SQL：
```bash
python3 starrocks_schema.py --config starrocks_config.json > schema.sql
mysql -h <fe_host> -P <fe_query_port> -u <user> -p < schema.sql
```
- 使用配置并发导入：
```bash
python3 starrocks_stream_load.py --config starrocks_config.json
```
- 显示进度：输出每表进度条，展示完成数、成功/失败及累计 `loaded_rows`/`filtered_rows`；可通过 `--progress` 控制显示。
- 并发说明：`--concurrency` 控制同一时刻并发的 Stream Load 数，默认 10。
- 可选净化：若历史分片仍带行末 `|`，可启用 `--sanitize` 按行净化再上传；新生成的数据已清理，可不启用以减少 CPU。

## Stream Load 并发导入
- 从数据目录读取所有表的所有分片，并发发起 `curl` PUT：
```bash
python3 starrocks_stream_load.py \
  --fe-host <fe_host> \
  --fe-port <fe_http_port> \
  --db tpch_db \
  --user <user> \
  --password <password> \
  --data-dir /home/disk1/liangchaohua/tpch/data/10GB \
  --concurrency 10
```
- 关键请求头：
  - `label:<table>_<timestamp>_<file>`（自动生成）
  - `Expect:100-continue`
  - `timeout:3600`（可调）
  - `max_filter_ratio:0.2`
  - `column_separator:|`

### 单文件加载示例（按配置的库名与指定表名）
```bash
curl --location-trusted -u '<username>':'<password>' \
  -H "Expect:100-continue" \
  -H "timeout:100" \
  -H "max_filter_ratio:0.2" \
  -H "column_separator:|" \
  -T example1.csv -XPUT \
  http://<fe_host>:<fe_http_port>/api/<dbname>/<tablename>/_stream_load
```
其中 `<dbname>` 来自 `starrocks_config.json` 的 `db`，`<tablename>` 为需要加载的表名（如 `lineitem`）。

## 性能与资源建议
- 并行生成和加载均会消耗 CPU、磁盘 IO、网络带宽；建议在高并发、高 IO 的主机执行。
- 超大规模（例如 10TB、100TB）会产生大量分片；可通过 `--concurrency` 调整 Stream Load 并发。
- 生成阶段会确保单文件不超过 5GB；如超限自动切分为 `*.part-xxxxx.tbl`。

## 常见问题
- `unknown url type: https`：Python 环境缺少 SSL；已实现自动回退到 `wget`/`curl` 下载。或手动传入 `--dbgen`。
- `dbgen directory not found`：网络镜像目录结构差异；脚本已改为递归查找 `Makefile` 并构建。
- `make not found`：安装 `make/gcc` 后重试。
- `curl: command not found`：安装 `curl` 或使用 `wget` 安装后再运行。
- Stream Load 失败：检查 FE HTTP 端口、用户名密码、数据库与表是否已创建、文件分隔符是否为 `|`。

## 验证
- 加载后可在 FE 执行简单行数检查：
```sql
SELECT COUNT(*) FROM tpch_db.lineitem;
SELECT COUNT(*) FROM tpch_db.orders;
```
## 运行 TPC-H 查询
- 使用 `tpch_run.py` 顺序执行 Q1～Q22，每个查询执行三次并记录结果与耗时；脚本会自动创建 Q15 所需视图 `revenue0`。
- 运行：
```bash
python3 tpch_run.py --config starrocks_config.json --runs 3 --outdir /home/disk1/liangchaohua/tpch/results
```
- 输出：
  - 每次结果：`<outdir>/Qxx/Qxx_run1.txt`、`Qxx_run2.txt`、`Qxx_run3.txt`
  - 汇总：`<outdir>/summary.csv`（run1/run2/run3 耗时与总耗时）

## 备注
- 所有生成数据均遵循 TPC-H 官方 `dbgen` 规则；字段顺序与建表语句匹配，无需重排列。
- 如需将输出文件扩展名统一改为 `.csv` 或增加协议选择（HTTPS/HTTP），可按需扩展脚本。
