from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests


def now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def log_line(log_path: Path, msg: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    line = f"[{now_ts()}] {msg}"
    print(line, flush=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def get_with_retries(
    url: str,
    params: Dict[str, Any],
    *,
    timeout: int = 90,
    verify: bool = False,
    max_retries: int = 6,
    sleep_base: float = 3.0,
    log_path: Optional[Path] = None,
) -> requests.Response:
    last_err: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout, verify=verify)
            if r.status_code == 200:
                return r
            last_err = RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            last_err = e

        sleep_s = sleep_base * attempt
        if log_path:
            log_line(log_path, f"retry {attempt}/{max_retries}: {last_err} (sleep {sleep_s:.1f}s)")
        time.sleep(sleep_s)

    raise RuntimeError(f"Failed after {max_retries} retries: {last_err}")


def is_geoserver_exception(text: str) -> bool:
    # GeoServer sometimes returns XML ServiceException with status=200
    t = text.lstrip()
    return t.startswith("<") and ("ServiceException" in t[:400] or "Exception" in t[:200])
