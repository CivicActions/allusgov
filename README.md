# Overview

This project attempts to map the organization of the US Federal Government by gathering and consolidating information from various directories.

[![PyPI License](https://img.shields.io/pypi/l/allusgov.svg)](https://pypi.org/project/allusgov)
[![PyPI Version](https://img.shields.io/pypi/v/allusgov.svg)](https://pypi.org/project/allusgov)
[![PyPI Downloads](https://img.shields.io/pypi/dm/allusgov.svg?color=orange)](https://pypistats.org/packages/allusgov)

Current sources:
* [SAM.gov Federal Hierarchy Public API](https://open.gsa.gov/api/fh-public-api/)
* [Federal Register Agencies API](https://www.federalregister.gov/developers/documentation/api/v1#/Agencies)
* [USA.gov A-Z Index of U.S. Government Departments and Agencies](https://www.usa.gov/federal-agencies)
* [OPM Federal Agencies List](https://www.opm.gov/about-us/open-government/Data/Apps/Agencies/)
* [CISA .gov data](https://github.com/cisagov/dotgov-data)
* [FY 2024 Federal Budget Outlays](https://www.govinfo.gov/app/details/BUDGET-2024-DB/BUDGET-2024-DB-2)
* [USASpending API Agencies & Sub-agencies](https://api.usaspending.gov/)

Each source is scraped (see [out](out) directory) in raw JSON format, including fields for the organizational unit name/parent (if any), unique ID/parent-ID fields (if the names are not unique) as well as any other attribute data for that organization available from that source.

A normalized name (still WIP) is then added, which corrects letter case, spacing and expands acronyms. Acronyms are selected and verified manually using data from [USCD GovSpeak](https://ucsd.libguides.com/govspeak) and the [DOD Dictionary of Military and Associated Terms](https://irp.fas.org/doddir/dod/dictionary.pdf) as well as manual entry when needed.

Each source is them imported into a tree and exported into the following formats for easy consumption:
* Plain text tree
* JSON flat format (with path to each element)
* JSON nested tree format
* CSV format (with embedded JSON attributes)
* Wide CSV format (with flattened attributes)
* [DOT file](https://en.wikipedia.org/wiki/DOT_(graph_description_language)) (does not include attributes)
* [GEXF graph file](https://gephi.org/gexf/format/) (includes flattened attributes)
* [GraphQL graph file](https://graphql.org/) (includes flattened attributes)
* [Cytoscape.js JSON format](https://js.cytoscape.org/#notation/elements-json) (includes flattened attributes)

To merge the lists, each tree is merged into a selected base tree by comparing the normalized names of each node in the tree to the names of each node in the base tree using a fuzzy matching algorithm. Similarity scores between each pair of parents are incorporated into the score to more correctly identify cases where the same/similar office or program name is used for different organizations.

Note that the fuzzy matching is imperfect and may have some inaccurate mappings (although most appear OK) and will certainly have some entries which actually should be merged, but aren't.

The final merged dataset is written in the above formats to the [data/merged](data/merged) directory.

## Setup

### Requirements

* Python 3.10+
* [Poetry](https://python-poetry.org/)

### Installation

Check out this repository, then from the repository root, install dependencies:

```text
$ poetry install
```

See command line usage:
```text
poetry run allusgov --help
```

Run a complete scrape and merge:
```text
poetry run allusgov
```