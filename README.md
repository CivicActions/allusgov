# All US Federal Government

This project attempts to map the organization of the US Federal Government by gathering and consolidating information from various directories.

Current sources:
* [USA.gov A-Z Index of U.S. Government Departments and Agencies](https://www.usa.gov/federal-agencies)
* [OPM Federal Agencies List](https://www.opm.gov/about-us/open-government/Data/Apps/Agencies/)
* [CISA .gov data](https://github.com/cisagov/dotgov-data)

Each source is scraped (see [out](out) directory) into a JSON format, including fields for the organizational unit name and parent name (if any).

To merge the lists, the organizational hierarchy "path" is generated, by following the parent fields. These "paths" are then fuzzy matched and (if a threshold is met) merged into a single entry.

Note that the fuzzy matching is imperfect and may have some inaccurate mappings (although most appear OK) and will certainly have some entries which actually should be merged, but aren't.

The final merged dataset is written in [JSON](out/merged.json) and flattened [CSV](out/merged.csv) format.