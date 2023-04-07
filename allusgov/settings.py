# Settings for allusgov project
#

from .exporter import exporter
from .importer import importer, samgov_importer
from .spider import budget, cisagov, opmgov, samgov, usagov, usaspending

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
}

# Exporter settings
EXPORTERS = {
    "text": exporter.TextExporter,
    "json": exporter.JSONExporter,
    "dot": exporter.DotExporter,
    "gexf": exporter.GEXFExporter,
    "graphml": exporter.GraphMLExporter,
    "cyjs": exporter.CytoscapeJSONExporter,
}

# Merge settings
MERGE_BASE = "samgov"