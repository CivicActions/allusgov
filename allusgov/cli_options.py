"""allusgov CLI options."""

import logging

import click
import click_log

from . import settings

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

# Global options decorator
def global_options(func):
    func = click_log.simple_verbosity_option(logger)(func)
    func = click.option(
        "--data-dir",
        default=settings.DATA_DIR,
        type=click.Path(
            exists=False, file_okay=False, writable=True, resolve_path=True
        ),
        help="Directory to store data and cache files",
    )(func)
    return func


def sources_options(func):
    func = click.argument(
        "sources",
        nargs=-1,
        callback=lambda ctx, param, value: value
        if value
        else list(settings.SOURCES.keys()),
    )(func)
    return func


# Spider options decorator
def spider_options(func):
    func = click.option(
        "--spider-page-limit",
        default=0,
        help="Limit the number of pages the spider should crawl",
    )(func)
    func = click.option(
        "--cache-dir",
        default=settings.CACHE_DIR,
        type=click.Path(
            exists=False, file_okay=False, writable=True, resolve_path=True
        ),
        help="Directory to store cache files",
    )(func)
    return func


# Export options decorator
def build_options(func):
    func = click.option(
        "--export/--no-export",
        " /-E",
        "to_export",
        default=True,
        help="Enable/disable actual exporting each source tree after building (default: True)",
    )(func)
    func = click.option(
        "--exporters",
        default=settings.EXPORTERS.keys(),
        multiple=True,
        help="Specify exporters to use",
    )(func)
    return func


# Merge options decorator
def merge_options(func):
    func = click.option(
        "--merge-base",
        default=settings.MERGE_BASE,
        help="Specify the base source for merging",
    )(func)
    func = click.option(
        "--merge-threshold",
        default=90,
        type=click.IntRange(min=0, max=100),
        help="Threshold for fuzzy string matching when merging (0-100)",
    )(func)
    return func


class CustomGroup(click.Group):
    def list_commands(self, ctx):
        # List the top level commands in a more helpful order.
        custom_order = ["all", "spider", "build", "merge", "dev"]
        return sorted(super().list_commands(ctx), key=custom_order.index)
