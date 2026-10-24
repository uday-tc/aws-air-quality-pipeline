"""Run the whole pipeline locally in one command:

    python -m pipeline.run_pipeline --source sample
    python -m pipeline.run_pipeline --source api      # real data (needs internet)

Each stage is retried with exponential back-off, mirroring Airflow's retry
policy, and a failure stops the run before bad data reaches the warehouse.
"""
from __future__ import annotations

import argparse
import logging
import time

from . import ingest, load, quality, transform

log = logging.getLogger("pipeline")


def with_retries(fn, name: str, attempts: int = 3, base_delay: float = 2.0):
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except quality.DataQualityError:
            raise  # retrying will not fix bad data
        except Exception as exc:
            if attempt == attempts:
                log.error("stage %s failed after %d attempts: %s", name, attempts, exc)
                raise
            delay = base_delay * 2 ** (attempt - 1)
            log.warning("stage %s failed (%s); retry %d/%d in %.0fs", name, exc, attempt, attempts, delay)
            time.sleep(delay)


def main(source: str = "sample") -> None:
    started = time.time()
    with_retries(lambda: ingest.ingest(source), "ingest")
    stats = with_retries(transform.run, "transform")
    with_retries(quality.run, "quality")
    counts = with_retries(load.run, "load")
    log.info("DONE in %.1fs | clean stats=%s | tables=%s", time.time() - started, stats, counts)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["api", "sample"], default="sample")
    main(ap.parse_args().source)
