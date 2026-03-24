# zambia-lots

Downloads the Zambia pending land lots WFS layer in paged chunks, merges the parts, and writes a clean `pending_lots.geoparquet`.

## How it works

1. **Fetch & chunk** — pages through the WFS endpoint 5000 features at a time, writing each page as a `.geoparquet` part file. Parts are written incrementally so a crash mid-run doesn't lose progress.
2. **Merge** — streams all parts into a single `pending_lots.geoparquet` via PyArrow, preserving GeoParquet geometry metadata.
3. **Clean** — drops null geometries and features whose centroid falls outside Zambia's bounding box.

## Run locally

From the repo root:

```bash
python src/pull_pending_lots.py \
  --base "https://geoserverprd.gsb.gov.zm/geoserver/wfs" \
  --layer "mlnr:pending-lots" \
  --out-dir "data/parts_pending_lots" \
  --out-final "data/pending_lots.geoparquet" \
  --progress-log "logs/progress.txt" \
  --page-size 5000 \
  --timeout 90 \
  --max-retries 6 \
  --sleep-base 3 \
  --crs "EPSG:3857" \
  --reproject-to "EPSG:4326"
```

Add `--resume` to pick up from the last completed part after a failure.

## Run on HPC (Slurm)

```bash
sbatch slurm/pull_pending_lots.slurm
```

## Checking progress

```bash
tail -f logs/progress_<JOBID>.txt
```
