import argparse
import math
import os
import shutil
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import urllib.request
import zipfile

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

SIZE_TO_SF = {
    "10GB": 10,
    "100GB": 100,
    "1TB": 1000,
    "10TB": 10000,
    "100TB": 100000,
}

EST_ROW_BYTES_MAX = {
    "customer": 240,
    "lineitem": 256,
    "nation": 64,
    "orders": 160,
    "part": 180,
    "partsupp": 240,
    "region": 64,
    "supplier": 200,
}

def ensure_dbgen(dbgen_path: Path, work_dir: Path) -> Path:
    if dbgen_path:
        p = Path(dbgen_path)
        if p.exists():
            return p
    found = shutil.which("dbgen")
    if found:
        return Path(found)
    tpch_repo_zip = "https://github.com/electrum/tpch-dbgen/archive/refs/heads/master.zip"
    download_dir = work_dir / "tpch-dbgen"
    download_dir.mkdir(parents=True, exist_ok=True)
    zip_path = download_dir / "tpch-dbgen.zip"
    try:
        urllib.request.urlretrieve(tpch_repo_zip, str(zip_path))
    except Exception:
        wget = shutil.which("wget")
        curl = shutil.which("curl")
        if wget:
            subprocess.run([wget, "-O", str(zip_path), tpch_repo_zip], check=True)
        elif curl:
            subprocess.run([curl, "-L", "-o", str(zip_path), tpch_repo_zip], check=True)
        else:
            raise RuntimeError("Failed to download dbgen. Install wget/curl or provide --dbgen path.")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(download_dir)
    make_dirs = []
    for p in download_dir.glob("**/"):
        if (p / "Makefile").exists() or (p / "makefile").exists():
            make_dirs.append(p)
    if not make_dirs:
        raise RuntimeError(f"build directory not found under {download_dir}")
    built_bin = None
    for md in make_dirs:
        try:
            subprocess.run(["make"], cwd=md, check=True)
            for b in md.glob("**/dbgen"):
                if b.is_file():
                    built_bin = b
                    break
            if built_bin:
                break
        except FileNotFoundError:
            raise RuntimeError("make not found. Please install make/gcc.")
        except subprocess.CalledProcessError:
            continue
    if not built_bin or not built_bin.exists():
        raise RuntimeError("dbgen build failed")
    return built_bin

def sf_from_size(size: str) -> int:
    if size not in SIZE_TO_SF:
        raise ValueError("Unsupported size")
    return SIZE_TO_SF[size]

def row_counts(sf: int) -> dict:
    return {
        "customer": 150000 * sf,
        "orders": 1500000 * sf,
        "lineitem": 6000000 * sf,
        "part": 200000 * sf,
        "partsupp": 800000 * sf,
        "supplier": 10000 * sf,
        "nation": 25,
        "region": 5,
    }

def compute_chunks(sf: int, max_file_size_gb: int) -> int:
    rc = row_counts(sf)
    bytes_per_row = EST_ROW_BYTES_MAX["lineitem"]
    total_bytes = rc["lineitem"] * bytes_per_row
    max_bytes = max_file_size_gb * (1024 ** 3)
    n = math.ceil(total_bytes / max_bytes)
    return max(1, n)

def run_dbgen_stream(dbgen_bin: Path, dists_path: Path, sf: int, total_streams: int, stream_id: int, out_tmp_dir: Path):
    cmd = [
        str(dbgen_bin),
        "-s",
        str(sf),
        "-C",
        str(total_streams),
        "-S",
        str(stream_id),
    ]
    if dists_path and Path(dists_path).exists():
        cmd += ["-b", str(dists_path)]
    cmd += ["-f"]
    subprocess.run(cmd, cwd=out_tmp_dir, check=True)

def move_stream_files(out_tmp_dir: Path, final_out_dir: Path, stream_id: int):
    for table in TABLES:
        src = out_tmp_dir / f"{table}.tbl.{stream_id}"
        if src.exists():
            dest_dir = final_out_dir / table
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest = dest_dir / f"{table}.tbl.{stream_id}"
            shutil.move(str(src), str(dest))
        else:
            if stream_id == 1:
                plain = out_tmp_dir / f"{table}.tbl"
                if plain.exists():
                    dest_dir = final_out_dir / table
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    dest = dest_dir / f"{table}.tbl.1"
                    shutil.move(str(plain), str(dest))

def split_if_needed(file_path: Path, max_bytes: int):
    sz = file_path.stat().st_size
    if sz <= max_bytes:
        return
    base = file_path.stem
    suffix = file_path.suffix
    parent = file_path.parent
    idx = 0
    written = 0
    out = None
    with open(file_path, "rb") as f:
        for line in f:
            if out is None or written >= max_bytes:
                if out is not None:
                    out.close()
                part_name = parent / f"{base}.part-{idx:05d}{suffix}"
                out = open(part_name, "wb")
                written = 0
                idx += 1
            out.write(line)
            written += len(line)
    if out is not None:
        out.close()
    file_path.unlink()

def sanitize_file(path: Path):
    tmp = Path(str(path) + ".san")
    with open(path, "rb") as fin, open(tmp, "wb") as fout:
        for line in fin:
            if line.endswith(b"|\n"):
                fout.write(line[:-2] + b"\n")
            elif line.endswith(b"|"):
                fout.write(line[:-1])
            else:
                fout.write(line)
    os.replace(tmp, path)

def verify_and_split(final_out_dir: Path, max_file_size_gb: int):
    max_bytes = max_file_size_gb * (1024 ** 3)
    for table in TABLES:
        tdir = final_out_dir / table
        if not tdir.exists():
            continue
        for p in tdir.glob("*.tbl.*"):
            sanitize_file(p)
            split_if_needed(p, max_bytes)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", required=True, choices=list(SIZE_TO_SF.keys()))
    parser.add_argument("--outdir", default="./out")
    parser.add_argument("--threads", type=int, default=max(1, os.cpu_count() or 1))
    parser.add_argument("--max-file-size-gb", type=int, default=5)
    parser.add_argument("--dbgen", default="")
    parser.add_argument("--chunks", type=int, default=0)
    args = parser.parse_args()
    out_root = Path(args.outdir).absolute()
    out_root.mkdir(parents=True, exist_ok=True)
    work_dir = Path.cwd()
    dbgen_bin = ensure_dbgen(Path(args.dbgen) if args.dbgen else None, work_dir)
    dists_path = None
    cand1 = dbgen_bin.parent / "dists.dss"
    cand2 = dbgen_bin.parent.parent / "dists.dss"
    if cand1.exists():
        dists_path = cand1
    elif cand2.exists():
        dists_path = cand2
    else:
        for p in dbgen_bin.parent.glob("**/dists.dss"):
            dists_path = p
            break
    sf = sf_from_size(args.size)
    streams = args.chunks if args.chunks and args.chunks > 0 else compute_chunks(sf, args.max_file_size_gb)
    tmp_dir = out_root / f"tmp_{args.size}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    final_dir = out_root / args.size
    final_dir.mkdir(parents=True, exist_ok=True)
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=args.threads) as ex:
        futs = []
        for s in range(1, streams + 1):
            futs.append(ex.submit(run_dbgen_stream, dbgen_bin, dists_path, sf, streams, s, tmp_dir))
        for f in as_completed(futs):
            pass
    with ThreadPoolExecutor(max_workers=args.threads) as ex:
        futs = []
        for s in range(1, streams + 1):
            futs.append(ex.submit(move_stream_files, tmp_dir, final_dir, s))
        for f in as_completed(futs):
            pass
    verify_and_split(final_dir, args.max_file_size_gb)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    print(str(final_dir))

if __name__ == "__main__":
    main()

