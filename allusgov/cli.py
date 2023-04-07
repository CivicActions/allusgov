"""allusgov CLI."""

import os
import sys
from typing import Dict, List, Optional, cast

import click
from bigtree import Node
from scrapy import signals
from scrapy.crawler import CrawlerProcess

from . import settings
from .cli_options import (
    logger,
    global_options,
    spider_options,
    build_options,
    merge_options,
    CustomGroup,
)
from .merger import merger
from .utils.utils import scrapy_settings, scrapy_spider_closed


@click.group(cls=CustomGroup)
@click.pass_context
def main(ctx):
    """
    Map the organization of the US Federal Government.

    This operates a three-stage pipeline:
    - Scrape the data from various directories (SOURCES)
    - Build a tree from the given SOURCES and export each source
    - Merge all data into a single tree, using fuzzy string matching

    Each stage is optional and will use cached data if available.
    """
    ctx.ensure_object(dict)


@main.command()
@global_options
@spider_options
def spider(sources: List[str], data_dir: str, spider_page_limit: int, cache_dir: str):
    """Scrape data from the specified sources."""
    spider_results: List[List[str]] = []
    process = CrawlerProcess(
        scrapy_settings(data_dir, cache_dir, spider_page_limit, logger)
    )
    for source in sources:
        os.makedirs(data_dir + "/" + source, exist_ok=True)
        spider_class = settings.SOURCES[source]["spider"]
        crawler = process.create_crawler(spider_class)
        callback = scrapy_spider_closed(spider_results)
        crawler.signals.connect(callback, signal=signals.spider_closed)
        process.crawl(crawler)
    process.start()
    for spider_name, reason in spider_results:
        if reason == "finished":
            logger.info("Spider %s finished successfully", spider_name)
        else:
            logger.error("Spider %s failed with reason: %s", spider_name, reason)
            sys.exit(10)


@main.command()
@global_options
@build_options
def build(
    sources: List[str], data_dir: str, exporters: List[str], to_export: bool
) -> Dict[str, Node]:
    """Build a tree from the given sources and optionally export each source."""
    tree = {}
    for source in sources:
        logger.info("Constructing the %s tree...", source)
        importer = settings.SOURCES[source]["importer"](
            logger=logger, source_name=source, data_dir=data_dir
        )
        tree[source] = importer.build()
        if to_export:
            for exporter in exporters:
                settings.EXPORTERS[exporter](
                    logger=logger, source=source, tree=tree[source], data_dir=data_dir
                ).export()
    return tree


@main.command()
@global_options
@merge_options
@build_options
@click.pass_context
def merge(
    ctx,
    sources: List[str],
    data_dir: str,
    merge_base: str,
    merge_threshold: int,
    exporters: List[str],
    to_export: bool,
    tree: Optional[Dict[str, Node]] = None,
):
    """Merge all data into a single tree using fuzzy string matching."""
    os.makedirs(data_dir + "/merged", exist_ok=True)
    if not tree:
        # If called directly, build the tree (without exporting)
        tree = cast(
            Dict[str, Node],
            ctx.invoke(
                build,
                sources=sources,
                data_dir=data_dir,
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
        for exporter in exporters:
            settings.EXPORTERS[exporter](
                logger=logger, source="merged", tree=base, data_dir=data_dir
            ).export()


@main.command(name="all")
@global_options
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
    "to_spider",
    default=True,
    help="Enable/disable merge step (default: True)",
)
@click.pass_context
def all_steps(
    ctx,
    sources: List[str],
    data_dir: str,
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
    if to_spider:
        # Execute spider step
        ctx.invoke(
            spider,
            sources=sources,
            data_dir=data_dir,
            spider_page_limit=spider_page_limit,
            cache_dir=cache_dir,
        )

    if to_export or to_merge:
        # Execute build step (merge depends on this)
        tree = ctx.invoke(
            build,
            sources=sources,
            data_dir=data_dir,
            exporters=exporters,
            export=to_export,
        )

    if to_merge:
        # Execute merge step
        ctx.invoke(
            merge,
            tree=tree,
            sources=sources,
            data_dir=data_dir,
            merge_base=merge_base,
            merge_threshold=merge_threshold,
        )


if __name__ == "__main__":  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter