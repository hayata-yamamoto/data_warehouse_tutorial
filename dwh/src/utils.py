import json
from datetime import datetime
from typing import Dict


def timestamp_to_datetime(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def load_json(fp: str) -> Dict[str, str]:
    with open(fp, "r") as f:
        data = json.load(f)
    return data
