# All US Federal Government

This project attempts to map the organization of the US Federal Government by gathering and consolidating information from various directories.

Current sources:
* [SAM.gov Federal Hierarchy Public API](https://open.gsa.gov/api/fh-public-api/)
* [USA.gov A-Z Index of U.S. Government Departments and Agencies](https://www.usa.gov/federal-agencies)
* [OPM Federal Agencies List](https://www.opm.gov/about-us/open-government/Data/Apps/Agencies/)
* [CISA .gov data](https://github.com/cisagov/dotgov-data)
* [FY 2024 Federal Budget Outlays](https://www.govinfo.gov/app/details/BUDGET-2024-DB/BUDGET-2024-DB-2)
* [USASpending API Agencies & Sub-agencies](https://api.usaspending.gov/)

Each source is scraped (see [out](out) directory) in raw JSON format, including fields for the organizational unit name/parent (if any), unique ID/parent-ID fields (if the names are not unique) as well as any other attribute data for that organization available from that source.

Each source is them imported into a tree and exported into the following formats for easy consumption:
* Plain text tree
* JSON flat format (with path to each element)
* JSON nested tree format
* [DOT file](https://en.wikipedia.org/wiki/DOT_(graph_description_language)) (does not include attributes)
* [GEXF graph file](https://gephi.org/gexf/format/) (includes flattened attributes)
* [GraphQL graph file](https://graphql.org/) (includes flattened attributes)
* [Cytoscape.js JSON format](https://js.cytoscape.org/#notation/elements-json) (includes flattened attributes)

To merge the lists, the organizational hierarchy "path" is generated, by following the parent fields. These "paths" are then fuzzy matched and (if a threshold is met) merged into a single entry.

Note that the fuzzy matching is imperfect and may have some inaccurate mappings (although most appear OK) and will certainly have some entries which actually should be merged, but aren't.

The final merged dataset is written in [JSON](out/merged.json) and flattened [CSV](out/merged.csv) format.