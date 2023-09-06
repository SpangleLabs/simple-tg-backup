import json
from typing import Dict


def load_config() -> Dict:
    with open("config.json") as f:
        return json.load(f)
