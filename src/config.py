from __future__ import annotations

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
CACHE_DIR = DATA_DIR / "cache"
SITE_DIR = PROJECT_ROOT / "docs" / "site"


def load_settings() -> dict:
    with open(CONFIG_DIR / "settings.yaml") as f:
        return yaml.safe_load(f)


def load_exclusion_chains() -> list[str]:
    chains = []
    path = CONFIG_DIR / "exclusion_chains.txt"
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                chains.append(line)
    return chains


# API keys from environment
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")
GOOGLE_APPS_SCRIPT_URL = os.getenv("GOOGLE_APPS_SCRIPT_URL", "")
