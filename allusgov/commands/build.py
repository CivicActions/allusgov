"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

from typing import cast

import click
from bigtree import Node, levelorder_iter

from allusgov import settings
from allusgov.cli_options import build_options, logger, sources_options


def build(sources: list[str], exporters: list[str], to_export: bool) -> dict[str, Node]:
    trees = {}
    for source in sources:
        logger.info("Constructing the %s tree...", source)
        importer = settings.SOURCES[source]["importer"](
            logger=logger, source_name=source, data_dir=settings.DATA_DIR
        )
        trees[source] = importer.build()
        if to_export:
            for exporter in exporters:
                settings.EXPORTERS[exporter](
                    logger=logger,
                    source=source,
                    tree=trees[source],
                    data_dir=settings.DATA_DIR,
                ).export()
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
    return build(sources, exporters, to_export)
