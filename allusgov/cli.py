"""allusgov CLI."""

from typing import Dict, List, Optional

import click
from bigtree import Node

from . import allusgov, settings
from .cli_options import (
    CustomGroup,
    build_options,
    global_options,
    merge_options,
    sources_options,
    spider_options,
)
from .dev import dev


@click.group(cls=CustomGroup)
@global_options
def main(data_dir: str):  # pylint: disable=unused-argument
    """
    Map the organization of the US Federal Government.

    This operates a three-stage pipeline:
    - Scrape the data from various directories (SOURCES)
    - Build a tree from the given SOURCES and export each source
    - Merge all data into a single tree, using fuzzy string matching

    Each stage is optional and will use cached data if available.
    """
    settings.DATA_DIR = data_dir


@main.command()
@sources_options
@spider_options
def spider(sources: List[str], spider_page_limit: int, cache_dir: str):
    """Scrape data from the specified sources."""
    allusgov.spider(sources, spider_page_limit, cache_dir)


@main.command()
@sources_options
@build_options
def build(sources: List[str], exporters: List[str], to_export: bool) -> Dict[str, Node]:
    """Build a tree for each of the given sources and optionally export each source."""
    return allusgov.build(sources, exporters, to_export)


@main.command()
@sources_options
@merge_options
@build_options
def merge(
    sources: List[str],
    merge_base: str,
    merge_threshold: int,
    exporters: List[str],
    to_export: bool,
    tree: Optional[Dict[str, Node]] = None,
):
    """Merge all data into a single tree using fuzzy string matching."""
    allusgov.merge(sources, merge_base, merge_threshold, exporters, to_export, tree)


@main.command(name="all")
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
def all_steps(
    sources: List[str],
    spider_page_limit: int,
    cache_dir: str,
    exporters: List[str],
    merge_base: str,
    merge_threshold: int,
    to_spider: bool,
    to_export: bool,
    to_merge: bool,
):
    """Execute all steps in order: spider, export, and merge."""
    allusgov.all_steps(
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


main.add_command(dev)

if __name__ == "__main__":  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
