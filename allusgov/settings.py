"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

from .processor import normalize_name

# Merge settings
MERGE_BASE = "samgov"

# Directories
DATA_DIR = "data"
CACHE_DIR = ".cache"

# Processors
POST_BUILD_PROCESSORS = [normalize_name.NormalizeName]
