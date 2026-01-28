# Settings for allusgov project
#

from .exporter import exporter
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
EXPORTERS = {
    "text": exporter.TextExporter,
    "json": exporter.JSONExporter,
    "csv": exporter.CSVExporter,
    "widecsv": exporter.WideCSVExporter,
    "dot": exporter.DotExporter,
    "gexf": exporter.GEXFExporter,
    "graphml": exporter.GraphMLExporter,
    "cyjs": exporter.CytoscapeJSONExporter,
}

# Merge settings
MERGE_BASE = "samgov"

# Directories
DATA_DIR = "data"
CACHE_DIR = ".cache"

# Processors
POST_BUILD_PROCESSORS = [normalize_name.NormalizeName]
