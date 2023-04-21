import re
from typing import Any, Dict, Iterator, List

import scrapy
from lxml import etree
from scrapy.http.request import Request
from scrapy.http.response.text import TextResponse


class USGovManualSpider(scrapy.Spider):
    name = "usgovmanual"
    urls = [
        "https://www.govinfo.gov/content/pkg/GOVMAN-2022-12-31/xml/GOVMAN-2022-12-31.xml",
        # Uncomment the following lines to get the previous years:
        # "https://www.govinfo.gov/content/pkg/GOVMAN-2021-12-22/xml/GOVMAN-2021-12-22.xml",
        # "https://www.govinfo.gov/content/pkg/GOVMAN-2020-11-10/xml/GOVMAN-2020-11-10.xml",
        # "https://www.govinfo.gov/content/pkg/GOVMAN-2019-11-21/xml/GOVMAN-2019-11-21.xml",
        # "https://www.govinfo.gov/content/pkg/GOVMAN-2018-12-03/xml/GOVMAN-2018-12-03.xml",
        # "https://www.govinfo.gov/content/pkg/GOVMAN-2017-08-02/xml/GOVMAN-2017-08-02.xml",
        # "https://www.govinfo.gov/content/pkg/GOVMAN-2016-12-16/xml/GOVMAN-2016-12-16.xml",
        # "https://www.govinfo.gov/content/pkg/GOVMAN-2015-07-01/xml/GOVMAN-2015-07-01.xml",
        # "https://www.govinfo.gov/content/pkg/GOVMAN-2014-10-06/xml/GOVMAN-2014-10-06.xml",
        # "https://www.govinfo.gov/content/pkg/GOVMAN-2013-11-06/xml/GOVMAN-2013-11-06.xml",
        # "https://www.govinfo.gov/content/pkg/GOVMAN-2012-12-07/xml/GOVMAN-2012-12-07.xml",
        # "https://www.govinfo.gov/content/pkg/GOVMAN-2011-10-05/xml/GOVMAN-2011-10-05.xml",
    ]
    snakecase = re.compile(r"(?<!^)(?=[A-Z])")

    def start_requests(self) -> Iterator[Request]:
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response: TextResponse, **kwargs: Any) -> Iterator[Dict[str, Any]]:
        root = etree.fromstring(response.body)
        for entity in root.findall("Entity"):
            yield self.entity_data(entity)
            for sub_entity_l1 in entity.findall("Childrens/SubEntityLevelOne"):
                yield self.entity_data(sub_entity_l1)
                for sub_entity_l2 in sub_entity_l1.findall(
                    "Childrens/SubEntityLevelTwo"
                ):
                    yield self.entity_data(sub_entity_l2)
                    for sub_entity_l3 in sub_entity_l2.findall(
                        "Childrens/SubEntityLevelThree"
                    ):
                        yield self.entity_data(sub_entity_l3)

    def add(
        self, element: etree.Element, data: Dict[str, Any], tag: str, key: str = ""
    ):
        """Lazy add element text to data dictionary."""
        if key == "":
            # Default key is tag name with camelcase converted to snakecase
            key = self.snakecase.sub("_", tag).lower()
        e = element.find(tag)
        if e is not None and e.text:
            if key in data:
                self.logger.error(f"Duplicate key {key} in data")
            data[key] = " ".join(e.text.strip().split())

    def entity_data(self, entity: etree.Element) -> Dict[str, Any]:
        entity_data = {
            "name": entity.find("AgencyName").text,
            "id": entity.attrib["EntityId"],
            "parent_id": entity.attrib["ParentId"],
        }
        if entity_data["parent_id"] == "0":
            entity_data["parent_id"] = None

        self.add(entity, entity_data, "Category")
        self.add(entity, entity_data, "IntroductoryParagraph", "introduction")
        self.add(entity, entity_data, "OrganizationChart")
        self.add_addresses(entity.find("Addresses"), entity_data)
        self.add_leadership(entity.find("LeaderShipTables"), entity_data)
        self.add_mission_statement(entity.find("MissionStatement"), entity_data)
        self.add_legal_authority(entity.find("LegalAuthority"), entity_data)
        self.add_program_and_activities(
            entity.find("ProgramAndActivities"), entity_data
        )
        self.add_source_of_information_details(
            entity.find("SourceOfInformationDetails"), entity_data
        )

        return entity_data

    def add_addresses(self, addresses: etree.Element, data: Dict[str, Any]):
        address_list = []

        for address_element in addresses.findall("Address"):
            address: Dict[str, Any] = {}
            self.add_footer_details(address_element, address)
            self.add(address_element, address, "Fax")
            self.add(address_element, address, "Phone")
            self.add(address_element, address, "Phone2")
            if address:
                address_list.append(address)
        if address_list:
            data["addresses"] = address_list

    def add_leadership(self, leadership: etree.Element, data: Dict[str, Any]):
        leadership_list = []

        for leadership_element in leadership.findall("LeaderShipTable"):
            leadership_table: Dict[str, Any] = {}
            self.add_footer_details(leadership_element, leadership_table)
            self.add(leadership_element, leadership_table, "Header")
            self.add(
                leadership_element, leadership_table, "HeaderParagraph", "introduction"
            )

            leaders_element = leadership_element.find("LeaderShipTableValues")
            if leaders_element is not None:
                leaders = []
                for leader_element in leaders_element.findall("Values"):
                    leader: Dict[str, Any] = {}
                    self.add(leader_element, leader, "NameColumnValue", "name")
                    self.add(leader_element, leader, "TitleColumnValue", "title")
                    # Skip some hard coded horizontal rules
                    if "name" in leader or (
                        "title" in leader and leader["title"].strip(" -")
                    ):
                        leaders.append(leader)
                leadership_table["listing"] = leaders
            leadership_list.append(leadership_table)
        if leadership_list:
            data["leadership"] = leadership_list

    def add_program_and_activities(
        self, program_and_activities: etree.Element, data: Dict[str, Any]
    ):
        pa_data = []

        pa_elements = program_and_activities.findall("ProgramAndActivity")
        for pa_element in pa_elements:
            pa: Dict[str, Any] = {}
            self.add_footer_details(pa_element, pa)
            self.add(pa_element, pa, "MainParagraph", "introduction")
            self.add(pa_element, pa, "PointOfContact")
            self.add(pa_element, pa, "ProgramName")

            programs: List[Dict[str, Any]] = []
            program_elements = pa_element.findall("Program")
            for program_element in program_elements:
                program_data: Dict[str, Any] = {}
                self.add(program_element, program_data, "Heading")
                self.add_details(program_element.find("Details"), program_data)
                if program_data:
                    programs.append(program_data)
            if programs:
                pa["programs"] = programs

            activities: List[Dict[str, Any]] = []
            activity_elements = pa_element.findall("Activity")
            for activity_element in activity_elements:
                activity_data: Dict[str, Any] = {}
                self.add(activity_element, activity_data, "Heading")
                self.add_details(activity_element.find("Details"), activity_data)
                self.add_key_official_tables(
                    activity_element.find("KeyOfficialTables"), activity_data
                )
                if activity_data:
                    activities.append(activity_data)
            if activities:
                pa["activities"] = activities
            if pa:
                pa_data.append(pa)
        if pa_data:
            data["program_and_activities"] = pa_data

    def add_details(self, details_element: etree.Element, data: Dict[str, Any]):
        details_data = []

        detail_elements = (
            details_element.findall("Detail") if details_element is not None else []
        )
        for detail_element in detail_elements:
            detail_data: Dict[str, Any] = {}
            self.add_footer_details(detail_element, detail_data)
            self.add(detail_element, detail_data, "Heading")
            self.add(detail_element, detail_data, "Paragraph", "text")
            if detail_data:
                details_data.append(detail_data)
        if details_data:
            data["details"] = details_data

    def add_key_official_tables(
        self, key_official_tables_element: etree.Element, data: Dict[str, Any]
    ):
        key_official_tables_data = []

        elements = (
            key_official_tables_element.findall("KeyOfficialTable")
            if key_official_tables_element is not None
            else []
        )
        persistent_headers: Dict[str, Any] = {}
        for element in elements:
            table: Dict[str, Any] = {}
            self.add(element, table, "TableHeader")
            self.add_footer_details(element, table)

            headers: Dict[str, Any] = {}
            self.add(element, headers, "ColumnOneHeader", "1")
            self.add(element, headers, "ColumnTwoHeader", "2")
            self.add(element, headers, "ColumnThreeHeader", "3")
            self.add(element, headers, "ColumnFourHeader", "4")
            self.add(element, headers, "ColumnFiveHeader", "5")
            self.add(element, headers, "ColumnSixHeader", "6")
            self.add(element, headers, "ColumnSevenHeader", "7")

            officials = self.get_key_official_table_values(
                element.find("KeyOfficialTableValues")
            )
            if not officials:
                if "2" in headers and "1" not in headers:
                    # Some tables are used as titles, but don't have any data
                    table["title"] = headers["2"]
                else:
                    # Others are used as persistent headers
                    persistent_headers = headers
                    continue

            for official in officials:
                for key, value in official.items():
                    header = key
                    if key in headers:
                        if key in persistent_headers:
                            header = persistent_headers[key] + "_" + headers[key]
                        else:
                            header = headers[key]
                    # pylint: disable-next=consider-using-get
                    elif key in persistent_headers:
                        header = persistent_headers[key]
                    header = re.sub("[^0-9a-zA-Z]+", "_", header).lower()
                    table[header] = value
            key_official_tables_data.append(table)
        if key_official_tables_data:
            data["key_officials"] = key_official_tables_data

    def get_key_official_table_values(
        self, key_official_tables_element: etree.Element
    ) -> List[Dict[str, Any]]:
        key_official_table_values_data = []

        elements = (
            key_official_tables_element.findall("Values")
            if key_official_tables_element is not None
            else []
        )
        for element in elements:
            data: Dict[str, Any] = {}
            self.add(element, data, "ColumnOneValue", "1")
            self.add(element, data, "ColumnTwoValue", "2")
            self.add(element, data, "ColumnThreeValue", "3")
            self.add(element, data, "ColumnFourValue", "4")
            self.add(element, data, "ColumnFiveValue", "5")
            self.add(element, data, "ColumnSixValue", "6")
            self.add(element, data, "ColumnSevenValue", "7")
            if data:
                key_official_table_values_data.append(data)

        return key_official_table_values_data

    def add_mission_statement(self, element: etree.Element, data: Dict[str, Any]):
        mission_statement_data: Dict[str, Any] = {}
        self.add_records(element.findall("Record"), mission_statement_data)
        self.add(element, mission_statement_data, "Heading")
        if mission_statement_data:
            data["mission_statement"] = mission_statement_data

    def add_legal_authority(self, legal_authority: etree.Element, data: Dict[str, Any]):
        legal_authority_data: Dict[str, Any] = {}
        self.add_records(legal_authority.findall("Record"), legal_authority_data)
        self.add(legal_authority, legal_authority_data, "Heading")
        if legal_authority_data:
            data["legal_authority"] = legal_authority_data

    def add_records(self, record_elements: etree.Element, data: Dict[str, Any]):
        records = []
        for record_element in record_elements:
            record: Dict[str, Any] = {}
            self.add_footer_details(record_element, record)
            self.add(record_element, record, "Paragraph", "text")
            if record:
                records.append(record)
        if records:
            data["records"] = records

    def add_footer_details(self, element: etree.Element, data: Dict[str, Any]):
        footer_details_element = element.find("FooterDetails")
        if footer_details_element is not None:
            self.add(footer_details_element, data, "Footer", "note")
            if "note" in data:
                data["note"] = data["note"].strip(" *")
                if not data["note"]:
                    del data["note"]
            self.add(footer_details_element, data, "Email")
            self.add(footer_details_element, data, "WebAddress", "url")

    def add_source_of_information_details(
        self, source_of_information_details: etree.Element, data: Dict[str, Any]
    ):
        entity_sources = []
        for entity_source_element in source_of_information_details.findall(
            "EntitySourceOfInformation"
        ):
            entity_source_data: Dict[str, Any] = {}
            self.add_footer_details(entity_source_element, entity_source_data)
            self.add(entity_source_element, entity_source_data, "Heading")
            self.add(entity_source_element, entity_source_data, "Paragraph", "text")
            if entity_source_data:
                entity_sources.append(entity_source_data)
        if entity_sources:
            data["source_of_information"] = entity_sources
