#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import argparse
import logging
from os.path import expanduser, join

from hdx.facades.keyword_arguments import facade
from hdx.api.configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir
from unhcr import failures, generate_dataset, get_dataset_ids

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-unhcr-microdata"


def main(output_failures=False, **ignore):
    configuration = Configuration.read()
    with Download() as downloader:
        dataset_ids = get_dataset_ids(configuration, downloader)
        logger.info(f"Number of datasets to upload: {len(dataset_ids)}")
        for info, dataset_id in progress_storing_tempdir(
            "UNHCR-MICRODATA", dataset_ids, "id"
        ):
            dataset = generate_dataset(
                dataset_id["id"], configuration, downloader, output_failures
            )
            if dataset:
                dataset.update_from_yaml()
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    hxl_update=False,
                    updated_by_script="HDX Scraper: UNHCR microdata",
                    batch=info["batch"],
                )
        for failure in failures:
            logger.error(failure)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UNHCR Microdata")
    parser.add_argument(
        "-of",
        "--output_failures",
        default=False,
        action="store_true",
        help="Output failures",
    )
    args = parser.parse_args()
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
        output_failures=args.output_failures,
    )
