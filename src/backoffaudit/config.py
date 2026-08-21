"""Load policy settings from a JSON or TOML file so a policy doesn't have
to be spelled out as flags every time it's reused."""

import json
import os

POLICY_KEYS = (
    "max_attempts",
    "base_delay",
    "multiplier",
    "max_delay",
    "jitter",
    "tolerance",
)


def load_policy_config(path: str) -> dict:
    """Returns a dict of whichever policy keys the file sets. Unknown keys
    are rejected so a typo in the config doesn't silently get ignored."""
    ext = os.path.splitext(path)[1].lower()
    with open(path, "rb") as fp:
        if ext == ".json":
            data = json.load(fp)
        elif ext == ".toml":
            try:
                import tomllib
            except ImportError:
                raise ValueError(
                    "TOML config files require Python 3.11+ (tomllib); "
                    "use a .json config on this interpreter"
                )
            data = tomllib.load(fp)
        else:
            raise ValueError(f"unrecognized config format: {path!r} (use .json or .toml)")

    if not isinstance(data, dict):
        raise ValueError(f"config file must contain a JSON/TOML object, got {type(data).__name__}")

    unknown = set(data) - set(POLICY_KEYS)
    if unknown:
        raise ValueError(f"unknown config key(s): {', '.join(sorted(unknown))}")

    return data
