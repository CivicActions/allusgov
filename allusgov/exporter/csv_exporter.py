"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

from bigtree import Node, tree_to_dataframe

from allusgov.cli_options import logger
from allusgov.models.exporter_base import ExporterBase
from allusgov.models.registry import EXPORTERS


@EXPORTERS.register("csv")
class CSVExporter(ExporterBase):
    """This results in more manageable CSV files, but the attributes end up as embedded JSON."""

    format_key = "csv"

    def __init__(self, source: str, tree: Node) -> None:
        super().__init__(source, tree)

    def export(self, **kwargs) -> None:
        logger.info("Saving the %s graph in CSV format...", self.source)
        df = tree_to_dataframe(self.tree, all_attrs=True)
        df.to_csv(self.export_path(ext="csv"), index=False)
