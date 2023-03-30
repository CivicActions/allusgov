import scrapy


class OpmgovSpider(scrapy.Spider):
    name = "opmgov"
    allowed_domains = ["opm.gov"]
    start_url = (
        "https://www.opm.gov/about-us/open-government/Data/Apps/agencies/agencies.xml"
    )

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_url,
            callback=self.parse,
            headers={"User-Agent": "Mozilla/5.0"},
        )

    def parse(self, response):
        for agency in response.xpath("//agency"):
            parent = agency.xpath("name/text()").get()
            if parent is None:
                parent = agency.xpath("dod_aggregate/text()").get().replace("D+-", "")
            name = agency.xpath("agency_subelement/text()").get()
            # Generate a path for the ID, to avoid collisions.
            id = parent + "/" + name
            # If an organization is its own parent, then it is a top-level organization.
            if name == parent:
                parent = None
                id = name
            yield {
                "parent": parent,
                "parent_id": parent,
                "type": agency.xpath("type/text()").get(),
                "name": name,
                "id": id,
                "employment": agency.xpath("employment/text()").get(),
            }
