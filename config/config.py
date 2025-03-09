from pathlib import Path
import yaml

root_folder = Path(__file__).parent.parent
file_config = root_folder / "config" / "config.yaml"

with open(file_config, "r") as file_:
    config = yaml.safe_load(file_)