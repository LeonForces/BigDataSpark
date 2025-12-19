import os
from typing import Optional


def env(name: str, default: Optional[str] = None, *, required: bool = False) -> Optional[str]:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return value
