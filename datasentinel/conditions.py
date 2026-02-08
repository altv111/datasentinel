import os
from typing import Dict


def _parse_properties(text: str) -> Dict[str, str]:
    conditions: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" in line:
            key, value = line.split(":", 1)
        elif "=" in line:
            key, value = line.split("=", 1)
        else:
            continue
        key = key.strip()
        value = value.strip()
        if not key or not value:
            continue
        conditions[key] = value
    return conditions


def load_conditions(base_path: str | None = None, user_path: str | None = None) -> Dict[str, str]:
    if base_path is None:
        base_path = os.path.join(os.path.dirname(__file__), "conditions.properties")
    conditions: Dict[str, str] = {}
    if os.path.exists(base_path):
        with open(base_path, "r") as f:
            conditions.update(_parse_properties(f.read()))

    if user_path is None:
        sentinel_home = os.getenv("SENTINEL_HOME")
        if sentinel_home:
            user_path = os.path.join(sentinel_home, "conditions.properties")
    if user_path and os.path.exists(user_path):
        with open(user_path, "r") as f:
            # User-defined conditions override built-ins.
            conditions.update(_parse_properties(f.read()))

    return conditions
