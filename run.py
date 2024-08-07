#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join
from traceback import format_exc

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.facades.simple import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.path import progress_storing_tempdir
from unhcr import UNHCR

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-unhcr-microdata"


def main():
    configuration = Configuration.read()
    with ErrorsOnExit() as errors:
        with Download(rate_limit={"calls": 1, "period": 5}) as downloader:
            unhcr = UNHCR(configuration, downloader)
            dataset_ids = unhcr.get_dataset_info()
            logger.info(f"Number of datasets to upload: {len(dataset_ids)}")
            for info, dataset_info in progress_storing_tempdir(
                "UNHCR-MICRODATA", dataset_ids, "id"
            ):
                dataset = unhcr.generate_dataset(dataset_info, errors)
                if dataset:
                    dataset.update_from_yaml()
                    try:
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            hxl_update=False,
                            updated_by_script="HDX Scraper: UNHCR microdata",
                            batch=info["batch"],
                        )
                    except HDXError:
                        url = dataset_info["url"]
                        logger.exception(f"Error with dataset: {url}!")
                        errors.add(f"Dataset: {url}, error: {format_exc()}")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml"),
    )
