"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

import click

from allusgov.commands.all_steps import all_steps_cmd
from allusgov.commands.build import build_cmd
from allusgov.commands.dev import dev
from allusgov.commands.merge import merge_cmd
from allusgov.commands.spider import spider_cmd
from allusgov.utils.logging_config import setup_logging

from . import settings
from .cli_options import CustomGroup, global_options


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
    setup_logging()
    settings.DATA_DIR = data_dir


main.add_command(spider_cmd)
main.add_command(build_cmd)
main.add_command(merge_cmd)
main.add_command(all_steps_cmd)
main.add_command(dev)


if __name__ == "__main__":  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
