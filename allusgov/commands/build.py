"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

from typing import cast

import click
from bigtree import Node, levelorder_iter

from allusgov import load_plugins_once
from allusgov.cli_options import build_options, logger, sources_options
from allusgov.config import settings
from allusgov.registry.managers import IMPORTERS, ExportManager, ImportManager
from allusgov.utils.utils import list_plugins_verbose


def build(sources: list[str], exporters: list[str], to_export: bool) -> dict[str, Node]:
    # Ensure importer/exporter plugins are registered before checking the registry below,
    # otherwise the very first source processed would incorrectly fall back to the generic
    # "importer" class, since IMPORTERS would still be empty at that point.
    load_plugins_once()
    trees = {}
    for source in sources:
        logger.info("Constructing the %s tree...", source)
        importers = list_plugins_verbose(registry=IMPORTERS)
        importer = source if source in importers else "importer"
        trees[source] = ImportManager(importer).run(source=source)
        exp = ExportManager()
        if to_export:
            for exporter in exporters:
                exp.export(
                    fmt=exporter,
                    source=source,
                    root=trees[source],
                )
        # Run post-build processors
        for processor_class in settings.POST_BUILD_PROCESSORS:
            processor = processor_class(logger, source, data_dir=settings.DATA_DIR)
            for org in levelorder_iter(trees[source]):
                org = cast(Node, org)
                processor.process(org)
    return trees


@click.command(name="build")
@sources_options
@build_options
def build_cmd(
    sources: list[str], exporters: list[str], to_export: bool
) -> dict[str, Node]:
    """Build a tree for each of the given sources and optionally export each source."""
    return build(sources=sources, exporters=exporters, to_export=to_export)
