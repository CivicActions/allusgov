import scrapy


class UsaspendingSpider(scrapy.Spider):
    name = "usaspending"
    allowed_domains = ["usaspending.gov"]
    base_url = "https://api.usaspending.gov/api/v2/"
    start_url = base_url + "references/toptier_agencies/"
    lookup = {}

    def request(self, url, callback):
        return scrapy.Request(
            url=url,
            callback=callback,
            # Need some headers to avoid Akamai blocks.
            headers={"User-Agent": "curl/7.87.0", "Accept": "*/*"},
        )

    def start_requests(self):
        yield self.request(self.start_url, self.parse_agencies)

    def subagency_url(self, agency_id, page=1):
        return (
            self.base_url
            + "agency/"
            + str(agency_id)
            + "/sub_agency/?page="
            + str(page)
        )

    def parse_agencies(self, response):
        agencies = response.json()["results"]
        for agency in agencies:
            yield self.request(
                self.subagency_url(agency["toptier_code"]), self.parse_subagencies
            )
            self.lookup[agency["toptier_code"]] = agency["agency_name"]
            agency["name"] = agency["agency_name"]
            agency["id"] = agency["toptier_code"]
            yield agency

    def parse_subagencies(self, response):
        self.logger.warning(response.text)
        self.logger.warning(response.request.headers)
        response = response.json()
        if response["page_metadata"]["hasNext"]:
            yield self.request(
                self.subagency_url(
                    response["toptier_code"], response["page_metadata"]["next"]
                ),
                self.parse_sub_agencies,
            )
        seen_subagency = set()
        for subagency in response["results"]:
            # Some subagency are duplicated, so we need to filter them out
            # https://github.com/fedspendingtransparency/usaspending-api/issues/3768
            if subagency["abbreviation"] in seen_subagency:
                continue
            seen_subagency.add(subagency["abbreviation"])
            for children in subagency["children"]:
                children["parent"] = subagency["name"]
                children["parent_id"] = response["toptier_code"] + "-" + subagency["abbreviation"]
                children["id"] = response["toptier_code"] + "-" + subagency["abbreviation"] + "-" + children["code"]
                yield children
            del subagency["children"]
            subagency["parent"] = self.lookup[response["toptier_code"]]
            subagency["parent_id"] = response["toptier_code"]
            subagency["id"] = response["toptier_code"] + "-" + subagency["abbreviation"]
            yield subagency
