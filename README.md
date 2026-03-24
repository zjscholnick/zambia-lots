# zambia-lots-wfs

Downloads a GeoServer WFS layer in paged chunks and writes GeoParquet parts + a merged final GeoParquet.

## Checking progress

tail -f logs/progress_<JOBID>.txt

## Run on HPC (Slurm)

From the repo root:

```bash
sbatch pull_pending_lots.slurm 


