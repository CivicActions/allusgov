"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

import sys

import click
from scrapy import signals
from scrapy.crawler import CrawlerProcess

from allusgov import settings
from allusgov.cli_options import logger, sources_options, spider_options
from allusgov.utils.utils import BASE_PATH, scrapy_settings, scrapy_spider_closed


def spider(sources: list[str], spider_page_limit: int, cache_dir: str):
    """Scrape data from the specified sources."""
    spider_results: list[list[str]] = []
    process = CrawlerProcess(
        scrapy_settings(settings.DATA_DIR, cache_dir, spider_page_limit, logger)
    )
    for source in sources:
        BASE_PATH.parent.joinpath(settings.DATA_DIR, source).mkdir(
            parents=True, exist_ok=True
        )
        crawler = process.create_crawler(source)
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


@click.command(name="spider")
@sources_options
@spider_options
def spider_cmd(sources: list[str], spider_page_limit: int, cache_dir: str):
    """Scrape data from the specified sources."""
    spider(sources, spider_page_limit, cache_dir)
