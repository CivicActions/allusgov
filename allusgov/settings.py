# Settings for allusgov project
from .processor import normalize_name
from .utils.yaml_loader import load_yaml_with_imports

config: dict = load_yaml_with_imports("allusgov/config.yaml")

# Source settings
SOURCES: dict = config.get("sources", {})

# Exporter settings
EXPORTERS: dict = config.get("exporters", {})

# Merge settings
MERGE_BASE = "samgov"

# Directories
DATA_DIR = "data"
CACHE_DIR = ".cache"

# Processors
POST_BUILD_PROCESSORS = [normalize_name.NormalizeName]
