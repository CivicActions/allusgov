from collections import OrderedDict
from typing import Any, Dict, Iterator, Union

import scrapy
from lxml import etree
from scrapy.http.request import Request
from scrapy.http.response.text import TextResponse


class USGovManualSpider(scrapy.Spider):
    name = "usgovmanual"
    start_url = "https://www.govinfo.gov/content/pkg/GOVMAN-2022-12-31/xml/GOVMAN-2022-12-31.xml"

    def start_requests(self) -> Iterator[Request]:
        yield scrapy.Request(url=self.start_url, callback=self.parse)

    def parse(self, response: TextResponse, **kwargs: Any) -> Iterator[Dict[str, Any]]:
        root = etree.fromstring(response.body)

        for element in root.iter():
            if element.tag in [
                "Entity",
                "SubEntityLevelOne",
                "SubEntityLevelTwo",
                "SubEntityLevelThree",
            ]:
                item = self.extract_element_data(element)
                yield item

    def extract_element_data(self, element: etree.Element) -> Dict[str, Any]:
        data: OrderedDict = OrderedDict()
        name = element.xpath("./AgencyName/text()")
        if not name:
            print(etree.tostring(element, encoding="unicode"))
            return data
        data["name"] = name[0]
        data["id"] = int(element.attrib["EntityId"])
        data["parent_id"] = (
            int(element.attrib["ParentId"]) if "ParentId" in element.attrib else None
        )

        for child in element.iterchildren():
            child_tag = self.cleanup_key(child.tag)
            if child_tag not in data:
                value = self.extract_child_data(child)
                if value is not None:
                    data[child_tag] = value
            else:
                if not isinstance(data[child_tag], list):
                    data[child_tag] = [data[child_tag]]
                data[child_tag].append(self.extract_child_data(child))

        return data

    @staticmethod
    def cleanup_key(key: str) -> str:
        return key.lower().replace(" ", "_")

    @staticmethod
    def remove_extra_spaces(s: str) -> str:
        return " ".join(s.strip().split())

    def extract_child_data(
        self, child: etree.Element
    ) -> Union[Dict[str, Any], str, None]:
        child_data = OrderedDict()
        if child.attrib:
            child_data.update(
                {
                    self.cleanup_key(str(k)): v
                    for k, v in child.attrib.items()
                    if v and not k.endswith("Id") and (k == "sortorder")
                }
            )

        if child.text and child.text.strip():
            child_data["text"] = self.remove_extra_spaces(child.text)

        for subchild in child.iterchildren():
            if not isinstance(subchild, etree._Comment):  # Ignore cyfunction comments
                self.process_subchild(child_data, subchild)

        if len(child_data) == 1 and "text" in child_data:
            return child_data["text"]
        elif child_data:
            return child_data
        else:
            return None

    def process_subchild(
        self, child_data: Dict[str, Any], subchild: etree.Element
    ) -> None:
        subchild_data = self.extract_child_data(subchild)
        if subchild_data is None:
            return
        subchild_tag = self.cleanup_key(str(subchild.tag))
        if subchild_tag == "numberofcolumnvisible" or subchild_tag.startswith("show"):
            return
        if subchild_tag in ("footerdetails", "record"):
            if isinstance(subchild_data, dict):
                for k, v in subchild_data.items():
                    if k in child_data:
                        self.logger.error(
                            f"Key {k} already exists in the parent level."
                        )
                    else:
                        child_data[k] = v
        else:
            if subchild_tag not in child_data:
                child_data[subchild_tag] = subchild_data
            else:
                if not isinstance(child_data[subchild_tag], list):
                    child_data[subchild_tag] = [child_data[subchild_tag]]
                child_data[subchild_tag].append(subchild_data)
