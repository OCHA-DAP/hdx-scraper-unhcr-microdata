#!/usr/bin/python
"""
UNHCR:
-----

Generates urls from the UNHCR microdata website.

"""
import logging

from dateutil.parser import ParserError
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date_range
from slugify import slugify

logger = logging.getLogger(__name__)


class UNHCR:
    def __init__(self, configuration, downloader):
        self.configuration = configuration
        self.downloader = downloader

    def get_dataset_info(self):
        url = f"{self.configuration['base_url']}{self.configuration['catalog_url']}"
        response = self.downloader.download(f"{url}latest?limit=10000")
        json = response.json()
        if not json.get("found"):
            raise ValueError("No datasets found!")
        dataset_info = list()
        for dataset in json["result"]:
            idno = dataset["idno"]
            if idno[:5] == "UNHCR":
                dataset_info.append(
                    {
                        "id": dataset["id"],
                        "changed": dataset["changed"],
                        "url": dataset["url"],
                    }
                )
            else:
                logger.info(f"Ignoring external dataset: {idno}")
        return dataset_info

    def generate_dataset(self, dataset_info, errors):
        dataset_id = dataset_info["id"]
        metadata_url = self.configuration["metadata_url"].format(dataset_id)
        json_url = f"{self.configuration['base_url']}{metadata_url}"
        response = self.downloader.download(json_url)
        json = response.json()
        study_desc = json["study_desc"]
        title_statement = study_desc["title_statement"]
        title = title_statement["title"]
        logger.info(f"Creating dataset: {title}")
        study_info = study_desc["study_info"]
        data_collection = study_desc["method"]["data_collection"]
        sources = [x["name"] for x in study_desc["authoring_entity"]]
        methodology = list()
        data_kind = study_info.get("data_kind")
        if data_kind is not None:
            methodology.append(f"Kind of Data: {data_kind}  \n")
        unit_analysis = study_info.get("universe")
        if unit_analysis is None:
            unit_analysis = study_info.get("analysis_unit")
        if unit_analysis is not None:
            methodology.append(f"Unit of Analysis: {unit_analysis}  \n")
        sampling = data_collection.get("sampling_procedure")
        if sampling is not None:
            methodology.append(f"Sampling Procedure: {sampling}  \n")
        collection = data_collection.get("coll_mode")
        if collection is not None:
            methodology.append(f"Data Collection Mode: {collection}  \n")
        dataset_name = slugify(title_statement["idno"])
        countryiso3s = set()
        for nation in study_info["nation"]:
            countryiso3 = nation["abbreviation"]
            if not countryiso3:
                countryname = nation["name"]
                if countryname:
                    countryiso3, _ = Country.get_iso3_country_code_fuzzy(countryname)
            if countryiso3:
                countryiso3s.add(countryiso3)
        if len(countryiso3s) == 1:
            countryname = Country.get_country_name_from_iso3(min(countryiso3s))
            title = f"{countryname} - {title}"
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": title,
                "notes": study_info["abstract"],
                "dataset_source": ", ".join(sources),
                "methodology": "Other",
                "methodology_other": "".join(methodology),
            }
        )
        dataset.set_maintainer("ac47b0c8-548b-4c37-a685-7377e75aad55")
        dataset.set_organization("abf4ca86-8e69-40b1-92f7-71509992be88")
        dataset.set_expected_update_frequency("Never")
        dataset.set_subnational(True)
        ui_url = dataset_info["url"]
        try:
            dataset.add_country_locations(countryiso3s)
        except HDXError:
            errors.add(
                f"Invalid country id {countryiso3s} in {ui_url}: {title}. ( JSON url {json_url} )!"
            )
            return None
        tags = list()

        def add_tags(inwords, key):
            for inword in inwords:
                inword = inword[key].strip().lower()
                if "," in inword:
                    words = inword.split(",")
                elif "/" in inword:
                    words = inword.split("/")
                else:
                    words = [inword]
                newwords = list()
                for innerword in words:
                    if "and" in innerword:
                        newwords.extend(innerword.split(" and "))
                    elif "&" in innerword:
                        newwords.extend(innerword.split(" & "))
                    elif "other" in innerword:
                        newwords.extend(innerword.split("other"))
                    else:
                        newwords.append(innerword)
                for word in newwords:
                    word = word.strip()
                    if word:
                        tags.append(word.strip())

        add_tags(study_info["topics"], "topic")
        add_tags(study_info.get("keywords", list()), "keyword")
        dataset.add_tags(tags)
        dataset.clean_tags()
        coll_dates = study_info["coll_dates"][0]
        try:
            startdate, _ = parse_date_range(coll_dates["start"])
            _, enddate = parse_date_range(coll_dates["end"])
        except ParserError:
            errors.add(
                f"Invalid date(s) in {ui_url}: {title}. ( JSON url {json_url} )!"
            )
            return None
        dataset.set_time_period(startdate, enddate)

        auth_url = self.configuration["auth_url"].format(dataset_id)
        resource = Resource(
            {
                "name": title,
                "description": 'Clicking "Download" leads outside HDX where you can request access to the data in csv, xlsx & dta formats',
                "url": f"{self.configuration['base_url']}{auth_url}",
                "format": "web app",
            }
        )
        changed = dataset_info["changed"]
        resource.set_date_data_updated(changed)
        dataset.add_update_resource(resource)

        documentation_url = self.configuration["documentation_url"].format(dataset_id)
        resource = Resource(
            {
                "name": "Codebook",
                "description": "Contains information about the dataset's metadata and data",
                "url": f"{self.configuration['base_url']}{documentation_url}",
                "format": "pdf",
            }
        )
        resource.set_date_data_updated(changed)
        dataset.add_update_resource(resource)

        return dataset
