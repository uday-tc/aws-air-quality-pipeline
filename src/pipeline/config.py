"""Central configuration for the pipeline.

Storage is abstracted by a base path so the same code runs against a local
folder (development) or an S3 prefix mirrored locally (production uses
`aws s3 sync` / the Terraform-provisioned bucket). Layers follow the
standard medallion layout: raw -> cleaned -> curated.
"""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", PROJECT_ROOT / "data"))

RAW_DIR = DATA_DIR / "raw"
CLEANED_DIR = DATA_DIR / "cleaned"
CURATED_DIR = DATA_DIR / "curated"
WAREHOUSE_PATH = DATA_DIR / "warehouse.duckdb"
QUALITY_REPORT_PATH = DATA_DIR / "quality_report.json"

# Indian cities used as the dimension seed (lat/lon are approximate city centres).
CITIES = {
    "Bengaluru": {"state": "Karnataka", "lat": 12.9716, "lon": 77.5946},
    "Hassan": {"state": "Karnataka", "lat": 13.0068, "lon": 76.1004},
    "Delhi": {"state": "Delhi", "lat": 28.6139, "lon": 77.2090},
    "Mumbai": {"state": "Maharashtra", "lat": 19.0760, "lon": 72.8777},
    "Chennai": {"state": "Tamil Nadu", "lat": 13.0827, "lon": 80.2707},
    "Kolkata": {"state": "West Bengal", "lat": 22.5726, "lon": 88.3639},
    "Hyderabad": {"state": "Telangana", "lat": 17.3850, "lon": 78.4867},
}

# Open-Meteo air-quality API (free, no API key needed).
API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
API_HOURLY_FIELDS = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "ozone"]

# Plausible physical ranges used by the data quality checks (ug/m3).
VALID_RANGES = {
    "pm10": (0, 1000),
    "pm2_5": (0, 1000),
    "carbon_monoxide": (0, 50000),
    "nitrogen_dioxide": (0, 2000),
    "ozone": (0, 1000),
}
