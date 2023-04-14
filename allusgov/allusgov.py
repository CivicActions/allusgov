"""allusgov library API."""

import os
import sys
from typing import Dict, List, Optional, cast

from bigtree import Node, levelorder_iter
from scrapy import signals
from scrapy.crawler import CrawlerProcess

from . import settings
from .cli_options import logger
from .merger import merger
from .utils.utils import scrapy_settings, scrapy_spider_closed


def spider(sources: List[str], spider_page_limit: int, cache_dir: str):
    """Scrape data from the specified sources."""
    spider_results: List[List[str]] = []
    process = CrawlerProcess(
        scrapy_settings(settings.DATA_DIR, cache_dir, spider_page_limit, logger)
    )
    for source in sources:
        os.makedirs(settings.DATA_DIR + "/" + source, exist_ok=True)
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


def build(sources: List[str], exporters: List[str], to_export: bool) -> Dict[str, Node]:
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


def merge(
    sources: List[str],
    merge_base: str,
    merge_threshold: int,
    exporters: List[str],
    to_export: bool,
    tree: Optional[Dict[str, Node]] = None,
):
    """Merge all data into a single tree using fuzzy string matching."""
    os.makedirs(settings.DATA_DIR + "/merged", exist_ok=True)
    if not tree:
        # If called directly, build the tree (without exporting)
        tree = cast(
            Dict[str, Node],
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
        for exporter in exporters:
            settings.EXPORTERS[exporter](
                logger=logger, source="merged", tree=base, data_dir=settings.DATA_DIR
            ).export()
    return base


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
    if to_spider:
        # Execute spider step
        spider(
            sources=sources,
            spider_page_limit=spider_page_limit,
            cache_dir=cache_dir,
        )

    if to_export or to_merge:
        # Execute build step (merge depends on this)
        trees = build(
            sources=sources,
            exporters=exporters,
            to_export=to_export,
        )

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
    return (base, trees)
