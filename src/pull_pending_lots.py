from __future__ import annotations

import argparse
import re
import time
from pathlib import Path

import geopandas as gpd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from shapely.geometry import shape

from utils import get_with_retries, is_geoserver_exception, log_line


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download WFS layer in chunks and merge to GeoParquet.")
    p.add_argument("--base", required=True, help="WFS endpoint, e.g. https://.../geoserver/wfs")
    p.add_argument("--layer", required=True, help="Layer name, e.g. mlnr:pending-lots")
    p.add_argument("--out-dir", required=True, help="Directory for chunk parts")
    p.add_argument("--out-final", required=True, help="Final merged GeoParquet path")
    p.add_argument("--progress-log", required=True, help="Progress log file path")

    p.add_argument("--page-size", type=int, default=5000)
    p.add_argument("--timeout", type=int, default=90)
    p.add_argument("--max-retries", type=int, default=6)
    p.add_argument("--sleep-base", type=float, default=3.0)
    p.add_argument("--verify", action="store_true", help="Verify SSL (default: off)")
    p.add_argument("--resume", action="store_true", help="Resume based on existing part files")
    p.add_argument(
        "--bbox",
        default="20.0,-20.0,36.0,-6.0",
        help="Cleanup bounding box as 'xmin,ymin,xmax,ymax' in the output CRS (default: generous Zambia extent)",
    )
    p.add_argument("--no-cleanup", action="store_true", help="Skip post-merge geometry cleanup")

    # Default source CRS for WFS geometry.
    p.add_argument(
        "--crs",
        default="EPSG:3857",
        help="CRS for geometry in GeoJSON",
    )
    p.add_argument("--reproject-to", help="Reproject to this CRS, e.g. EPSG:4326")
    return p.parse_args()


def get_total_hits(base: str, layer: str, log_path: Path, **reqkw) -> int:
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": layer,
        "resultType": "hits",
    }
    log_line(log_path, f"Querying hits for {layer} ...")
    r = get_with_retries(base, params, log_path=log_path, **reqkw)

    m = re.search(r'numberMatched="(\d+)"', r.text)
    if not m:
        raise RuntimeError(f"Could not find numberMatched. First 500 chars:\n{r.text[:500]}")
    total = int(m.group(1))
    log_line(log_path, f"GeoServer reports numberMatched={total:,}")
    return total


def infer_resume_start(out_dir: Path, page_size: int, log_path: Path) -> tuple[int, int]:
    parts = sorted(out_dir.glob("part-*.geoparquet"))
    if not parts:
        return 0, 0
    last = parts[-1].stem  # part-000123
    try:
        part_idx = int(last.split("-")[1]) + 1
        start_index = part_idx * page_size
        log_line(log_path, f"Resuming: found {len(parts)} parts. startIndex={start_index}, part_idx={part_idx}")
        return start_index, part_idx
    except Exception:
        log_line(log_path, "Found parts but couldn't infer resume point. Starting from scratch.")
        return 0, 0


def fetch_page(base: str, layer: str, start: int, count: int, log_path: Path, **reqkw) -> dict:
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": layer,
        "outputFormat": "application/json",
        "count": str(count),
        "startIndex": str(start),
    }
    r = get_with_retries(base, params, log_path=log_path, **reqkw)
    if is_geoserver_exception(r.text):
        raise RuntimeError(f"GeoServer exception at startIndex={start}: {r.text[:800]}")
    return r.json()


def write_chunk_geoparquet(data: dict, out_path: Path, crs: str, reproject_to: str | None = None) -> int:
    feats = data.get("features", [])
    if not feats:
        return 0

    rows = []
    geoms = []
    for f in feats:
        rows.append(f.get("properties", {}))
        geoms.append(shape(f["geometry"]) if f.get("geometry") else None)

    gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs=crs)
    if reproject_to:
        gdf = gdf.to_crs(reproject_to)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(out_path, engine="pyarrow", compression="zstd", index=False)
    return len(gdf)


def merge_parts(parts_dir: Path, out_final: Path, log_path: Path) -> int:
    parts = sorted(parts_dir.glob("part-*.geoparquet"))
    if not parts:
        raise RuntimeError("No parts found to merge.")

    # Preserve GeoParquet metadata from first file
    first_pf = pq.ParquetFile(parts[0])
    schema = first_pf.schema_arrow
    meta = schema.metadata or {}

    dataset = ds.dataset(str(parts_dir), format="parquet")
    scanner = dataset.scanner(columns=schema.names, batch_size=65536)

    out_final.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(str(out_final), schema.with_metadata(meta), compression="zstd")

    merged = 0
    batches = 0
    t0 = time.time()
    for batch in scanner.to_batches():
        writer.write_table(pa.Table.from_batches([batch], schema=schema))
        merged += batch.num_rows
        batches += 1
        if batches % 20 == 0:
            log_line(log_path, f"MERGE progress: merged={merged:,} batches={batches:,}")
    writer.close()

    log_line(log_path, f"MERGE done -> {out_final} rows={merged:,} elapsed={time.time()-t0:.1f}s")
    return merged


def clean_final(
    path: Path,
    log_path: Path,
    bbox: tuple[float, float, float, float],
) -> int:
    """Drop null geometries and features whose centroid falls outside bbox.

    Overwrites *path* in-place with the cleaned result.
    Returns the number of rows kept.
    """
    xmin, ymin, xmax, ymax = bbox
    log_line(log_path, f"CLEAN reading {path} ...")
    gdf = gpd.read_parquet(path)
    n_start = len(gdf)

    # 1. Drop null geometries
    null_mask = gdf.geometry.isna()
    n_null = int(null_mask.sum())
    if n_null:
        log_line(log_path, f"CLEAN dropping {n_null:,} null-geometry rows")
        gdf = gdf[~null_mask].copy()

    # 2. Drop features whose centroid falls outside the bounding box
    cx = gdf.geometry.centroid.x
    cy = gdf.geometry.centroid.y
    outside = ~((cx >= xmin) & (cx <= xmax) & (cy >= ymin) & (cy <= ymax))
    n_outside = int(outside.sum())
    if n_outside:
        bad_ids = gdf.loc[outside, "UPID"].tolist() if "UPID" in gdf.columns else []
        log_line(
            log_path,
            f"CLEAN dropping {n_outside:,} out-of-bbox rows: {bad_ids[:20]}"
            + (" ..." if len(bad_ids) > 20 else ""),
        )
        gdf = gdf[~outside].copy()

    n_kept = len(gdf)
    n_dropped = n_start - n_kept
    log_line(log_path, f"CLEAN done: kept={n_kept:,} dropped={n_dropped:,} (null={n_null}, bbox={n_outside})")

    gdf.to_parquet(path, engine="pyarrow", compression="zstd", index=False)
    return n_kept


def main() -> None:
    args = parse_args()

    base = args.base
    layer = args.layer
    out_dir = Path(args.out_dir)
    out_final = Path(args.out_final)
    log_path = Path(args.progress_log)

    reqkw = dict(
        timeout=args.timeout,
        verify=args.verify,
        max_retries=args.max_retries,
        sleep_base=args.sleep_base,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    log_line(log_path, f"START base={base} layer={layer} page_size={args.page_size} out_dir={out_dir}")

    total = get_total_hits(base, layer, log_path, **reqkw)

    start_index = 0
    part_idx = 0
    if args.resume:
        start_index, part_idx = infer_resume_start(out_dir, args.page_size, log_path)

    written = start_index
    t0 = time.time()

    for start in range(start_index, total, args.page_size):
        out_part = out_dir / f"part-{part_idx:06d}.geoparquet"
        if args.resume and out_part.exists():
            log_line(log_path, f"Skipping existing {out_part.name}")
            part_idx += 1
            continue

        log_line(log_path, f"Fetch startIndex={start} count={args.page_size} -> {out_part.name}")
        data = fetch_page(base, layer, start, args.page_size, log_path, **reqkw)
        n = write_chunk_geoparquet(data, out_part, args.crs, args.reproject_to)

        if n == 0:
            log_line(log_path, f"No features returned at startIndex={start}. Stopping early.")
            break

        written += n
        part_idx += 1

        elapsed = time.time() - t0
        rate = written / elapsed if elapsed > 0 else 0.0
        log_line(log_path, f"WROTE {out_part.name}: n={n:,} total_written≈{written:,}/{total:,} rate={rate:,.1f} rows/s")

    log_line(log_path, "Download finished. Starting merge...")
    merge_parts(out_dir, out_final, log_path)

    if not args.no_cleanup:
        bbox_vals = tuple(float(v) for v in args.bbox.split(","))
        clean_final(out_final, log_path, bbox_vals)

    log_line(log_path, "ALL DONE.")


if __name__ == "__main__":
    main()
