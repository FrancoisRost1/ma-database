"""
Config loader — reads config.yaml once and returns it as a dict.
Passed down through all modules; never imported directly from yaml elsewhere.
"""
import yaml
from pathlib import Path


def load_config(path: str = "config.yaml") -> dict:
    """Load config.yaml from the given path and return as a nested dict."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml not found at: {config_path.resolve()}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
