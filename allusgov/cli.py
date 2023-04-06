"""allusgov CLI."""

import logging
import os
import sys
from typing import List

import click
import click_log
from scrapy import signals
from scrapy.crawler import CrawlerProcess

from . import settings
from .merger import merger
from .utils.utils import scrapy_settings, scrapy_spider_closed

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


@click.command()
@click_log.simple_verbosity_option(logger)
@click.argument("sources", nargs=-1)
@click.option("--spider/--no-spider", " /-S", default=True)
@click.option("--spider-page-limit", default=0)
@click.option(
    "--export/--no-export",
    " /-E",
    default=True,
    help="Export each source tree. (default: True)",
)
@click.option("--exporters", default=settings.EXPORTERS.keys(), multiple=True)
@click.option("--merge/--no-merge", " /-M", default=True)
@click.option("--merge-base", default=settings.MERGE_BASE)
@click.option("--merge-threshold", default=90, type=click.IntRange(min=0, max=100))
@click.option(
    "--data-dir",
    default="data",
    type=click.Path(exists=False, file_okay=False, writable=True, resolve_path=True),
)
@click.option(
    "--cache-dir",
    default=".cache",
    type=click.Path(exists=False, file_okay=False, writable=True, resolve_path=True),
)
def main(
    sources: list,
    spider: bool,
    spider_page_limit: int,
    export: bool,
    exporters: list,
    merge: bool,
    merge_base: str,
    merge_threshold: int,
    data_dir: str,
    cache_dir: str,
):
    """
    Map the organization of the US Federal Government.

    This operates a three stage pipeline:
    - Scrape the data from from various directories (SOURCES)
    - Build a tree from the given SOURCES and export each source
    - Merge all data into a single tree, using fuzzy string matching

    Each stage is optional and will use cached data if available.
    """
    # Initialize variables
    if not sources:
        sources = list(settings.SOURCES.keys())

    # Validate options
    if merge and merge_base not in sources:
        raise click.BadParameter(
            "The merge base must be one of the sources: " + ", ".join(sources)
        )

    # Create data directories
    for source in sources:
        os.makedirs(data_dir + "/" + source, exist_ok=True)

    # Scrape data
    if spider:
        spider_results: List[List[str]] = []
        process = CrawlerProcess(
            scrapy_settings(data_dir, cache_dir, spider_page_limit, logger)
        )
        for source in sources:
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

    # Construct and export trees
    tree = {}
    for source in sources:
        logger.info("Constructing the %s tree...", source)
        importer = settings.SOURCES[source]["importer"](
            logger=logger, source_name=source, data_dir=data_dir
        )
        tree[source] = importer.build()
        if export:
            for exporter in exporters:
                settings.EXPORTERS[exporter](
                    logger=logger, source=source, tree=tree[source], data_dir=data_dir
                ).export()

    # Merge trees
    if merge:
        os.makedirs(data_dir + "/merged", exist_ok=True)
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
        for exporter in exporters:
            settings.EXPORTERS[exporter](
                logger=logger, source="merged", tree=base, data_dir=data_dir
            ).export()


if __name__ == "__main__":  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
