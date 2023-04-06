from datetime import datetime
from typing import Any, Dict, Iterator, Optional, Union

import scrapy
from scrapy.http.request import Request
from scrapy.http.response.text import TextResponse
from w3lib.url import add_or_replace_parameters, url_query_parameter


class SamgovSpider(scrapy.Spider):
    name = "samgov"
    allowed_domains = ["sam.gov"]
    limit = 100
    base_url = "https://api.sam.gov/prod/federalorganizations/v1/orgs"

    def url(self, url: str = base_url, params: Optional[Dict[str, str]] = None) -> str:
        if params is None:
            params = {}
        params.update(
            {
                "api_key": self.settings.getdict("DOTENV")["SAM_API_KEY"],
                "limit": str(self.limit),
            }
        )
        return add_or_replace_parameters(url, params)

    def start_requests(self) -> Iterator[Request]:
        yield scrapy.Request(url=self.url(), callback=self.parse)

    def parse(
        self, response: TextResponse, **kwargs: Any
    ) -> Iterator[Union[Request, Dict[str, Any],]]:
        data = response.json()
        if (
            url_query_parameter(response.url, "offset") is None
            and data["totalrecords"] > self.limit
        ):
            # No existing offset, fetch additional pages.
            for offset in range(self.limit, data["totalrecords"], self.limit):
                yield response.follow(
                    self.url(response.url, {"offset": str(offset)}), callback=self.parse
                )
        for org in data["orglist"]:
            if org["status"] == "ACTIVE" and "fhorgname" in org:
                for link in org["links"]:
                    if link["rel"] == "nextlevelchildren":
                        yield response.follow(
                            self.url(link["href"]), callback=self.parse
                        )
                # Extract and follow IDs from the parent history - this shouldn't be
                # neccessary, but the API is (perhaps) inconsistent or perhaps some
                # items have inactive parents?
                if "fhorgparenthistory" in org:
                    latest_entry = None
                    if len(org["fhorgparenthistory"]) == 1:
                        # If there is only a single entry we just use that.
                        latest_entry = org["fhorgparenthistory"][0]
                    else:
                        # Identify the most recent entry.
                        latest_date = datetime.min
                        for entry in org["fhorgparenthistory"]:
                            effective_date = datetime.strptime(
                                entry["effectivedate"], "%Y-%m-%d %H:%M"
                            )
                            if effective_date > latest_date:
                                latest_date = effective_date
                                latest_entry = entry
                    if latest_entry:
                        for parent_id in latest_entry["fhfullparentpathid"].split("."):
                            yield response.follow(
                                self.url(self.base_url + "?fhorgid=" + parent_id),
                                callback=self.parse,
                            )
                yield org
