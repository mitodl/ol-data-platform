from pathlib import Path
from typing import Any, Dict, Union

import yaml


def load_yaml_config(config_path: Union[Path, str]) -> Dict[str, Any]:
    """Load a YAML config from disk if it exists.

    :param config_path: Path to the config file formatted as YAML
    :type config_path: Path

    :return: The configuration dictionary parsed from the YAML file if it exists.  If it
             doesn't exist, return an empty dictionary.
    :rtype: Dict[str, Any]
    """
    config_path = Path(config_path)
    if not config_path.exists():
        return {}
    with config_path.open("r") as yaml_config:
        return yaml.safe_load(yaml_config)
