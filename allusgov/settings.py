# Settings for allusgov project
#

from allusgov.utils.yaml_loader import load_yaml_with_imports

from .importer import importer, samgov_importer
from .processor import normalize_name
from .spider import (
    budget,
    cisagov,
    federalregister,
    opmgov,
    samgov,
    usagov,
    usaspending,
    usgovmanual,
)

config: dict = load_yaml_with_imports("allusgov/config.yaml")

# Source settings
SOURCES = {
    "samgov": {
        "importer": samgov_importer.SamgovImporter,
        "spider": samgov.SamgovSpider,
    },
    "budget": {
        "importer": importer.Importer,
        "spider": budget.BudgetSpider,
    },
    "cisagov": {
        "importer": importer.Importer,
        "spider": cisagov.CisagovSpider,
    },
    "opmgov": {
        "importer": importer.Importer,
        "spider": opmgov.OpmgovSpider,
    },
    "usagov": {
        "importer": importer.Importer,
        "spider": usagov.UsagovSpider,
    },
    "usaspending": {
        "importer": importer.Importer,
        "spider": usaspending.UsaspendingSpider,
    },
    "federalregister": {
        "importer": importer.Importer,
        "spider": federalregister.FederalRegisterSpider,
    },
    "usgovmanual": {
        "importer": importer.Importer,
        "spider": usgovmanual.USGovManualSpider,
    },
}

# Exporter settings
EXPORTERS: dict = config.get("exporters", {})

# Merge settings
MERGE_BASE = "samgov"

# Directories
DATA_DIR = "data"
CACHE_DIR = ".cache"

# Processors
POST_BUILD_PROCESSORS = [normalize_name.NormalizeName]
