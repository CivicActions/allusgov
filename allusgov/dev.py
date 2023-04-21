import json
import re
from typing import Any, Dict, List, cast

import click
import questionary
from bigtree import Node, levelorder_iter
from scrapy.crawler import CrawlerProcess

from . import allusgov, settings
from .cli_options import logger, spider_options
from .spider.acronyms import DoDAcronymsSpider, GovSpeakAcronymsSpider
from .utils.utils import scrapy_settings

acronym_sources = {
    "govspeak": GovSpeakAcronymsSpider,
    "dod": DoDAcronymsSpider,
}

# Create an exception type for exiting from the TUI
class ExitTUI(Exception):
    pass


@click.group()
def dev():
    """Development commands."""
    pass


@dev.command()
@spider_options
def acronyms_spider(spider_page_limit: int, cache_dir: str):
    """Spider acronyms for all sources."""
    spider_settings = scrapy_settings(
        settings.DATA_DIR, cache_dir, spider_page_limit, logger
    )
    spider_settings.set(
        "FEEDS",
        {
            settings.DATA_DIR
            + "/acronyms/%(spider_name)s-raw.json": {
                "format": "json",
                "encoding": "utf8",
                "overwrite": True,
            },
        },
    )
    process = CrawlerProcess(spider_settings)
    for spider in acronym_sources.values():
        process.crawl(spider)
    process.start()  # the script will block here until the crawling is finished
    for key in acronym_sources:
        with open(
            f"{settings.DATA_DIR}/acronyms/{key}-raw.json", "r", encoding="utf-8"
        ) as f:
            acronym_list = json.load(f)
        acronyms = {}
        # Resturcture into a dict of acronyms to a dict of expansions to data
        # since this makes lookups easier.
        for acronym in acronym_list:
            expansions = {}
            for data in acronym["expansions"]:
                expansion = data["expansion"]
                del data["expansion"]
                expansions[expansion] = data
            acronyms[acronym["acronym"]] = expansions
        with open(
            f"{settings.DATA_DIR}/acronyms/{key}.json", "w", encoding="utf-8"
        ) as f:
            json.dump(acronyms, f, indent=2)


def acronym_fetch(
    path_ids: List[str],
    expansion: str,
    library: Dict[str, Any],
) -> Dict[str, Any]:
    """Fetch an acronym from the acronym library, adding specified ID."""
    item = {}
    if "source" in library[expansion]:
        item["link"] = library[expansion]["source"]
    if "link" in library[expansion]:
        item["link"] = library[expansion]["link"]
    if "ids" in library[expansion]:
        item["ids"] = list(set(library[expansion]["ids"] + path_ids))
    else:
        item["ids"] = path_ids
    return item


def acronym_custom(
    acronym: str,
) -> Dict[str, Any]:
    """Ask the user for a custom expansion."""
    expansion = questionary.text(f'Type the expansion for "{acronym}":').ask()
    link = questionary.text(f'Enter a source link (optional) for "{acronym}":').ask()
    item = {"expansion": expansion, "source": "allusgov"}
    if link:
        item["link"] = link
    return item


def acronym_resolve(
    acronym: str,
    path_ids: List[str],
    library: Dict[str, Any],
) -> Dict[str, Any]:
    """Determine an expansion for an acronym."""
    print("\n")
    print("Acronym: ", acronym)
    print("IDs and mappings:")
    # Retrieve any existing mappings for this acronym for reference
    acronyms = {}
    for path_id in path_ids:
        mapped = False
        for expansion, values in library.items():
            if "ids" in values and path_id in values["ids"]:
                acronyms[expansion] = values
                print(f"Mapped: {path_id} to '{expansion}'")
                mapped = True
        if not mapped:
            print(f"Unmapped: {path_id}")
    # Output the possible expansions for this acronym
    print("Possible expansions:")
    for expansion, values in library.items():
        print(f"  {expansion}")
    choices = []
    choices.append(
        questionary.Choice(
            "Ignore this acronym and treat as normal word",
            value="ignore",
            shortcut_key="1",
        )
    )
    if len(library.keys()) > 0:
        choices.append(
            questionary.Choice(
                "Apply 1 expansion to all IDs", value="all", shortcut_key="2"
            )
        )
        choices.append(
            questionary.Choice(
                "Apply 1 expansion to 1 or more IDs", value="some", shortcut_key="3"
            )
        )
    choices.append(
        questionary.Choice(
            "Apply custom expansion to 1 or more IDs", value="custom", shortcut_key="4"
        )
    )
    choices.append(questionary.Choice("Save and exit", value="exit", shortcut_key="x"))
    choice = questionary.select(
        f'Select approach for "{acronym}"',
        choices=choices,
        use_shortcuts=True,
    ).ask()
    if choice == "exit":
        raise ExitTUI
    if choice == "ignore":
        return {"ignore": True}
    if choice == "custom":
        item = acronym_custom(acronym)
        expansion = item["expansion"]
        library[expansion] = item
    if choice in ("all", "some") and len(library.keys()) > 1:
        expansion = questionary.select(
            "Select expansion to use",
            choices=list(library.keys()),
            use_shortcuts=True,
        ).ask()
        selected_path_ids = path_ids
    else:
        # Only a single expansion exists, so use it
        expansion = list(library.keys())[0]
    if choice in ("some", "custom") and len(path_ids) > 1:
        # Ask the user to select which IDs to apply the expansion to
        selected_path_ids = questionary.checkbox(
            f'Select IDs to apply "{expansion}" to for "{acronym}"',
            choices=path_ids,
        ).ask()
    else:
        # Only a single ID exists, so use it
        selected_path_ids = path_ids

    # Build acronym dictionary from selected items
    acronyms[expansion] = acronym_fetch(selected_path_ids, expansion, library)
    return acronyms


def acronyms_load_library(acronyms: Dict[str, Any]) -> Dict[str, Any]:
    """Load acronyms library from file and existing acronyms."""
    acronym_library = {}
    for source in acronym_sources:
        try:
            with open(
                f"{settings.DATA_DIR}/acronyms/{source}.json", "r", encoding="utf-8"
            ) as f:
                for acronym, values in json.load(f).items():
                    # Ensure all keys are uppercase
                    acronym_library[acronym.upper()] = values
        except FileNotFoundError:
            pass

    # List of abbreviations
    # TODO: this can go away once included in resolved acronyms.
    abbreviations = f"{settings.DATA_DIR}/acronyms/abbreviations.json"
    try:
        with open(abbreviations, "r", encoding="utf-8") as f:
            for acronym, expansion in json.load(f).items():
                # Ensure all keys are uppercase
                acronym_library[acronym.upper()] = {}
                acronym_library[acronym.upper()][expansion] = {"source": "allusgov"}
    except FileNotFoundError:
        pass

    # Include resolved acronyms in the acronym library, so that we can reuse custom expansions
    for acronym, values in acronyms.items():
        if acronym not in acronym_library:
            acronym_library[acronym] = {}
        for expansion, values in values.items():
            acronym_library[acronym][expansion] = values
    return acronym_library


@dev.command()
def acronyms_selector():
    """Interatively select acronyms from directory and store results."""
    # Execute build step
    trees = allusgov.build(
        sources=list(settings.SOURCES.keys()),
        exporters=[],
        to_export=False,
    )
    # Load acronym dictionary - library and resolved data use identical format.
    # Key: acronym
    # Value is a dictionary with:
    #   Key: either
    #       "expansion": text definition/replacement
    #       "ignore": ignore this acronym (treat as proper word) and skip in future
    #   Value is a dictionary with:
    #       "link": link to organization or definition
    #       "source": source of this expansion (govspeak, allusgov, etc.)
    #       "ids": list of IDs this has been mapped to
    acronyms_file = f"{settings.DATA_DIR}/acronyms/acronyms.json"
    try:
        with open(acronyms_file, "r", encoding="utf-8") as f:
            acronyms = json.load(f)
    except FileNotFoundError:
        acronyms = {}

    acronym_library = acronyms_load_library(acronyms)
    to_resolve: Dict[str, List[str]] = {}
    for source, tree in trees.items():
        for org in levelorder_iter(tree):
            org = cast(Node, org)
            attrs = org.get_attr(source)
            name = attrs["normalized_name"]
            for acronym in re.findall(r"\b[A-Z]{2,}\b", name):
                if acronym in acronyms.keys():
                    # Skip ignored acronyms and already mapped IDs
                    if "ignore" in acronyms[acronym]:
                        continue
                    mapped = False
                    for values in acronyms[acronym].values():
                        if org.path_name in values["ids"]:
                            mapped = True
                    if mapped:
                        continue
                # If it is a new acronym or unmapped, add it to the list of acronyms to resolve
                if acronym not in to_resolve:
                    to_resolve[acronym] = []
                to_resolve[acronym].append(org.path_name)
    # Resolve acronyms
    total = len(to_resolve.keys())
    count = 0
    for acronym, path_ids in to_resolve.items():
        if acronym not in acronym_library:
            library = {}
        else:
            library = acronym_library[acronym]
        try:
            print(f"({count} completed of {total} acronyms")
            acronyms[acronym] = acronym_resolve(acronym, path_ids, library)
            with open(acronyms_file, "w", encoding="utf-8") as f:
                json.dump(acronyms, f, indent=2)
        except ExitTUI:
            with open(acronyms_file, "w", encoding="utf-8") as f:
                json.dump(acronyms, f, indent=2)
            return
