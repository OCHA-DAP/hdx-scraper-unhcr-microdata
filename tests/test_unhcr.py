#!/usr/bin/python
"""
Unit tests for UNHCR microdata.

"""
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.loader import load_json

from unhcr import generate_dataset, get_dataset_ids

dataset_ids_json = {
    "limit": 10000,
    "found": 85,
    "result": [
        {
            "id": "187",
            "idno": "UNHCR-AFG-2017-SEA_KhostPaktika-1.1",
            "title": "Socio-economic assessment of Pakistani refugees in Afghanistan's Khost and Paktika provinces 2017",
            "nation": "Afghanistan",
            "created": "Dec-05-2019",
            "changed": "Dec-05-2019",
            "url": "https://microdata.unhcr.org/index.php/catalog/187",
        },
        {
            "id": "272",
            "idno": "UNHCR_PHL_2016_Zamboanga_HB_IDP_Profiling",
            "title": "Zamboanga Home Based IDP Re-Profiling 2016",
            "nation": "Philippines",
            "created": "Sep-28-2020",
            "changed": "Sep-28-2020",
            "url": "https://microdata.unhcr.org/index.php/catalog/272",
        },
    ],
}
metadata_187 = load_json(join("tests", "fixtures", "metadata_187.json"))
metadata_272 = load_json(join("tests", "fixtures", "metadata_272.json"))


class TestUNHCR:
    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            user_agent="test",
            hdx_key="12345",
            project_config_yaml=join("tests", "config", "project_configuration.yml"),
        )
        Locations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
                {"name": "phl", "title": "Philippines"},
            ]
        )
        Country.countriesdata(use_live=False)
        return Configuration.read()

    @pytest.fixture(scope="function")
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if "limit=10000" in url:

                    def fn():
                        return dataset_ids_json

                    response.json = fn
                elif "187" in url:

                    def fn():
                        return metadata_187

                    response.json = fn
                elif "272" in url:

                    def fn():
                        return metadata_272

                    response.json = fn
                return response

        return Download()

    def test_get_datasetids(self, configuration, downloader):
        assert get_dataset_ids(configuration, downloader) == [
            {"id": "187"},
            {"id": "272"},
        ]

    def test_generate_dataset(self, configuration, downloader):
        dataset = generate_dataset(
            "187", configuration, downloader
        )
        assert dataset == {
            "name": "unhcr-afg-2017-sea-khostpaktika-1-1",
            "title": "Afghanistan - Socio-economic assessment of Pakistani refugees in Afghanistan's Khost and Paktika provinces 2017",
            "notes": "Afghanistan hosts a protracted population of Pakistani refugees, who fled North Waziristan Agency in 2014 as a result of a joint military offensive by Pakistani government forces against non-state armed groups. As of May 2017, UNHCR has biometrically registered over 50,000 refugees in Khost province and 36,000 refugees in Paktika province, where access remains a challenge. Over 16,000 of these refugees receive shelter and essential services in the Gulan camp in Khost province, while most of the others live among the host population in various urban and rural locations. \nTo better understand the needs of the refugees and the host communities, UNHCR and WFP agreed to conduct a joint assessment of Pakistani refugees in Khost and Paktika. The data collection commenced in May 2017 and covered 2,638 refugee households (2,198 in Khost and 440 in Paktika).",
            "dataset_source": "Office of the High Commissioner for Refugees",
            "methodology": "Other",
            "methodology_other": "Kind of Data: Sample survey data [ssd]  \nUnit of Analysis: All Pakistani refugees living in Afghanistan's Khost and Paktika provinces.\n\nUNHCR PPG: -  \nSampling Procedure: The survey's objective was to deliver representative data of all Pakistani refugees living in Afghanistan's Khost and Paktika provinces. The total population of Pakistani refugees in these provinces at the time of the survey was estimated at around 18,000 households.  \n\nFor this survey a stratified, two-stage (i.e.  clustered) sample design was applied. The 10 refugee-hosting districts of Khost and Paktika were considered sampling strata, but within these the refugee-dense locations of Gulan camp and Lakan (in Maton district) were considered separate strata, resulting in 12 sampling strata overall. Within each of these strata, first a selection of villages was drawn with probability-proportional-to-size, then second a selection of households was drawn from UNHCR's registration database. \n\nThe total sample size was 2,638 refugee households. \n\nNB: The original data collection also included a small number of households from the neighbouring host communities; however, these observations were dropped from the public-release version of the dataset.  \nData Collection Mode: Face-to-face [f2f]  \n",
            "maintainer": "ac47b0c8-548b-4c37-a685-7377e75aad55",
            "owner_org": "abf4ca86-8e69-40b1-92f7-71509992be88",
            "data_update_frequency": "-1",
            "subnational": "1",
            "groups": [{"name": "afg"}],
            "tags": [
                {
                    "name": "livelihoods",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
                {
                    "name": "food security",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
                {
                    "name": "refugees",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
                {
                    "name": "asylum seekers",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
                {
                    "name": "displacement",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
                {
                    "name": "violence and conflict",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
            ],
            "dataset_date": "[2017-05-11T00:00:00 TO 2017-05-29T00:00:00]",
        }
        resources = dataset.get_resources()
        assert resources == [
            {
                "name": "Afghanistan - Socio-economic assessment of Pakistani refugees in Afghanistan's Khost and Paktika provinces 2017",
                "description": 'Clicking "Download" leads outside HDX where you can request access to the data in csv, xlsx & dta formats',
                "url": "https://lala/index.php/auth/login/?destination=catalog/187/get-microdata",
                "format": "web app",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "Codebook",
                "description": "Contains information about the dataset's metadata and data",
                "url": "https://lala/index.php/catalog/187/pdf-documentation",
                "format": "pdf",
                "resource_type": "api",
                "url_type": "api",
            },
        ]

        dataset = generate_dataset(
            "272", configuration, downloader
        )
        assert dataset == {
            "name": "unhcr-phl-2016-zamboanga-hb-idp-profiling",
            "title": "Philippines - Zamboanga Home Based IDP Re-Profiling 2016",
            "notes": "In April 2016, following a series of consultations between the United Nations High Commissioner for Refugees, the City Social Welfare and Development Office and other partners in Zamboanga, a profiling exercise for home-based internally displaced persons (IDPs) was conceptualized. The main purpose was to validate the relevance of existing lists and obtain up-to-date information from home-based IDPs who decided to take part in the exercise so that the government, as well as other humanitarian and development actors, can make informed and consultative decisions while designing and targeting their assistance programs, including protection interventions. \nFollowing a piloting phase in June 2016, the full-blown profiling was conducted in July-August 2016 and reached 6,474 families from 66 barangays in Zamboanga. Of these, 1,135 families were assessed to be potential home-based IDPs based on the documents they presented. The profiling revealed that most home-based IDPs are living in barangays of Sta. Catalina, Sta. Barbara, Talon-Talon and Rio Hondo.",
            "dataset_source": "UNHCR",
            "methodology": "Other",
            "methodology_other": 'Kind of Data: Census/enumeration data [cen]  \nUnit of Analysis: All suspected households hosting IDPs. \nIdentification and verification of home-based IDPs during profiling were fully contingent on the presentation of following documentation by families approaching the profiling team: Family Access Cards (colored pink) issued by DSWD for home-based IDPs at the onset of the siege, Certification issued by CSWDO indicating the family as home-based IDPs, as well as National Housing Authority\'s (NHA) Tagging Form or Barangay Chairman endorsement provided that these two latter documents are presented together with a Family Access Card (colored pink) or CSWDO certification as home-based IDP.  \nSampling Procedure: The current CSWDO master list and the results of profiling in December 2014, which made up the "consolidated master list", were used to help identify location of the IDPs. Based on this consolidated master list, a total of 4,372 families was used as baseline data. This home-based IDP profiling activity was able to reach 6,474 families from 66 barangays.  \nData Collection Mode: Face-to-face [f2f]  \n',
            "maintainer": "ac47b0c8-548b-4c37-a685-7377e75aad55",
            "owner_org": "abf4ca86-8e69-40b1-92f7-71509992be88",
            "data_update_frequency": "-1",
            "subnational": "1",
            "groups": [{"name": "phl"}],
            "tags": [
                {
                    "name": "vulnerable populations",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
                {
                    "name": "displaced persons locations - camps - shelters",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
                {
                    "name": "non-food items - nfi",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
                {
                    "name": "livelihoods",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
                {
                    "name": "gender-based violence - gbv",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
                {
                    "name": "mental health",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
                {
                    "name": "housing",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
                {
                    "name": "needs assessment",
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                },
            ],
            "dataset_date": "[2016-07-01T00:00:00 TO 2016-08-31T00:00:00]",
        }
        resources = dataset.get_resources()
        assert resources == [
            {
                "name": "Philippines - Zamboanga Home Based IDP Re-Profiling 2016",
                "description": 'Clicking "Download" leads outside HDX where you can request access to the data in csv, xlsx & dta formats',
                "url": "https://lala/index.php/auth/login/?destination=catalog/272/get-microdata",
                "format": "web app",
                "resource_type": "api",
                "url_type": "api",
            },
            {
                "name": "Codebook",
                "description": "Contains information about the dataset's metadata and data",
                "url": "https://lala/index.php/catalog/272/pdf-documentation",
                "format": "pdf",
                "resource_type": "api",
                "url_type": "api",
            },
        ]
