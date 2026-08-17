"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

from typing import cast

import click
from bigtree import Node

from allusgov.cli_options import build_options, logger, merge_options, sources_options
from allusgov.commands.build import build
from allusgov.config import settings
from allusgov.merger import merger
from allusgov.registry.managers import ExportManager


def merge(
    sources: list[str],
    merge_base: str,
    merge_threshold: int,
    exporters: list[str],
    to_export: bool,
    tree: dict[str, Node] | None = None,
):
    """Merge all data into a single tree using fuzzy string matching."""
    settings.BASE_PATH.parent.joinpath(settings.DATA_DIR, "merged").mkdir(
        parents=True, exist_ok=True
    )
    if not tree:
        # If called directly, build the tree (without exporting)
        tree = cast(
            dict[str, Node],
            build(
                sources=sources,
                exporters=exporters,
                to_export=False,
            ),
        )
    base = tree[merge_base]
    for source in sources:
        if source == merge_base:
            continue
        logger.info("Merging in the %s tree...", source)
        base = merger.Merger(
            logger=logger,
            base_tree=base,
            base_name=merge_base,
            source_tree=tree[source],
            source_name=source,
            threshold=merge_threshold,
        ).merge()
    if to_export:
        exp = ExportManager()
        for exporter in exporters:
            exp.export(fmt=exporter, source="merged", root=base)
    return base


@click.command(name="merge")
@sources_options
@merge_options
@build_options
def merge_cmd(
    sources: list[str],
    merge_base: str,
    merge_threshold: int,
    exporters: list[str],
    to_export: bool,
    tree: dict[str, Node] | None = None,
):
    """Merge all data into a single tree using fuzzy string matching."""
    merge(sources, merge_base, merge_threshold, exporters, to_export, tree)
