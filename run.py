#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir

from unhcr import generate_dataset, get_dataset_ids

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-unhcr-microdata'


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    catalog_url = configuration['catalog_url']
    metadata_url = configuration['metadata_url']
    documentation_url = configuration['documentation_url']
    auth_url = configuration['auth_url']
    with Download() as downloader:
        dataset_ids = get_dataset_ids(catalog_url, downloader)
        logger.info('Number of datasets to upload: %d' % len(dataset_ids))
        for info, dataset_id in progress_storing_tempdir('UNHCR-MICRODATA', dataset_ids, 'id'):
            dataset = generate_dataset(dataset_id['id'], metadata_url, auth_url, documentation_url, downloader)
            if dataset:
                dataset.update_from_yaml()
                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: UNHCR microdata', batch=info['batch'])


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
