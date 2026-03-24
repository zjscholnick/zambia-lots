#!/bin/bash
#SBATCH --job-name=zambia_pending_lots_wfs
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=06:00:00
#SBATCH --output=logs/%j.out
#SBATCH --error=logs/%j.err

set -euo pipefail

REPO="/home/zscholnick/zambia-lots"
ENV="/home/zscholnick/miniforge3/envs/rs-ml-env"

mkdir -p "$REPO/logs" "$REPO/data"

# Activate conda env (works even in non-interactive Slurm shells)
source "/home/zscholnick/miniforge3/etc/profile.d/conda.sh"
conda activate "$ENV"

BASE="https://geoserverprd.gsb.gov.zm/geoserver/wfs"
LAYER="mlnr:pending-lots"

OUT_DIR="$REPO/data/parts_pending_lots"
OUT_FINAL="$REPO/data/pending_lots.geoparquet"
PROGRESS_LOG="$REPO/logs/progress_${SLURM_JOB_ID}.txt"

PAGE_SIZE="5000"

python "$REPO/src/pull_pending_lots.py" \
  --base "$BASE" \
  --layer "$LAYER" \
  --out-dir "$OUT_DIR" \
  --out-final "$OUT_FINAL" \
  --progress-log "$PROGRESS_LOG" \
  --page-size "$PAGE_SIZE" \
  --timeout 90 \
  --max-retries 6 \
  --sleep-base 3 \
  --crs "EPSG:3857" \
  --reproject-to "EPSG:4326"
