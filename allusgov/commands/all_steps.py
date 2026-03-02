"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

import click

from allusgov.cli_options import (
    build_options,
    merge_options,
    sources_options,
    spider_options,
)
from allusgov.commands.build import build
from allusgov.commands.merge import merge
from allusgov.commands.spider import spider


def all_steps(
    sources: list[str],
    spider_page_limit: int,
    cache_dir: str,
    exporters: list[str],
    merge_base: str,
    merge_threshold: int,
    to_spider: bool,
    to_export: bool,
    to_merge: bool,
) -> tuple[object | None, dict[str, object] | None]:
    """Execute all steps in order: spider, export, and merge."""
    if to_spider:
        # Execute spider step
        spider(
            sources=sources,
            spider_page_limit=spider_page_limit,
            cache_dir=cache_dir,
        )

    trees: dict[str, object] | None = None
    if to_export or to_merge:
        # Execute build step (merge depends on this)
        trees = build(
            sources=sources,
            exporters=exporters,
            to_export=to_export,
        )

    base: object | None = None
    if to_merge:
        # Execute merge step
        base = merge(
            tree=trees,
            sources=sources,
            merge_base=merge_base,
            merge_threshold=merge_threshold,
            exporters=exporters,
            to_export=to_export,
        )
    return base, trees


@click.command(name="all")
@sources_options
@spider_options
@build_options
@merge_options
@click.option(
    "--spider/--no-spider",
    " /-S",
    "to_spider",
    default=True,
    help="Enable/disable spider step (default: True)",
)
@click.option(
    "--merge/--no-merge",
    " /-M",
    "to_merge",
    default=True,
    help="Enable/disable merge step (default: True)",
)
def all_steps_cmd(
    sources: list[str],
    spider_page_limit: int,
    cache_dir: str,
    exporters: list[str],
    merge_base: str,
    merge_threshold: int,
    to_spider: bool,
    to_export: bool,
    to_merge: bool,
):
    """Execute all steps in order: spider, export, and merge."""
    all_steps(
        sources,
        spider_page_limit,
        cache_dir,
        exporters,
        merge_base,
        merge_threshold,
        to_spider,
        to_export,
        to_merge,
    )
