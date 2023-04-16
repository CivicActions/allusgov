from typing import Any, Callable, Dict, Iterator, Union

import scrapy
from scrapy.http.request import Request
from scrapy.http.response.text import TextResponse


class UsaspendingSpider(scrapy.Spider):
    name = "usaspending"
    allowed_domains = ["usaspending.gov"]
    base_url = "https://api.usaspending.gov/api/v2/"
    start_url = base_url + "references/toptier_agencies/"
    lookup: Dict[str, str] = {}

    def request(self, url: str, callback: Callable) -> Request:
        return scrapy.Request(
            url=url,
            callback=callback,
            # Need some headers to avoid Akamai blocks.
            headers={"User-Agent": "curl/7.87.0", "Accept": "*/*"},
        )

    def start_requests(self) -> Iterator[Request]:
        yield self.request(self.start_url, self.parse)

    def subagency_url(self, agency_id: str, page: int = 1) -> str:
        return (
            self.base_url
            + "agency/"
            + str(agency_id)
            + "/sub_agency/?page="
            + str(page)
        )

    def parse(
        self, response: TextResponse, **kwargs: Any
    ) -> Iterator[Union[Request, Dict[str, Union[int, str, None, float]],]]:
        """Handle the top level of agencies which each need their own request."""
        agencies = response.json()["results"]
        for agency in agencies:
            yield self.request(
                self.subagency_url(agency["toptier_code"]), self.parse_subagencies
            )
            self.lookup[agency["toptier_code"]] = agency["agency_name"]
            agency["name"] = agency["agency_name"]
            agency["id"] = agency["toptier_code"]
            yield agency

    def parse_subagencies(
        self, response: TextResponse
    ) -> Iterator[Dict[str, Union[int, str, None, float]]]:
        """Handle the subagencies for a given agency as well as their child organizations."""
        response = response.json()
        if response["page_metadata"]["hasNext"]:
            yield self.request(
                self.subagency_url(
                    response["toptier_code"], response["page_metadata"]["next"]
                ),
                self.parse_subagencies,
            )
        seen_subagency = set()
        for subagency in response["results"]:
            # Some subagencies are duplicated, so we need to filter them out
            # https://github.com/fedspendingtransparency/usaspending-api/issues/3768
            abbreviation = subagency["abbreviation"]
            if abbreviation is None:
                # Very occasionally subagencies don't have an abbreviation, so we use the name
                abbreviation = subagency["name"]
            if abbreviation in seen_subagency:
                continue
            seen_subagency.add(abbreviation)
            subagency["parent"] = self.lookup[response["toptier_code"]]
            subagency["parent_id"] = response["toptier_code"]
            subagency["id"] = response["toptier_code"] + "-" + abbreviation
            for child in subagency["children"]:
                child["parent"] = subagency["name"]
                child["parent_id"] = subagency["id"]
                child["id"] = (
                    response["toptier_code"] + "-" + abbreviation + "-" + child["code"]
                )
                yield child
            del subagency["children"]
            yield subagency
