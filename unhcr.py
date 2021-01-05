#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
UNHCR:
-----

Generates urls from the UNHCR microdata website.

"""
import logging

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from slugify import slugify

logger = logging.getLogger(__name__)


def get_dataset_ids(catalog_url, downloader):
    response = downloader.download(f'{catalog_url}latest?limit=10000')
    json = response.json()
    if not json.get('found'):
        raise ValueError('No datasets found!')
    return [{'id': x['id']} for x in json['result']]


def generate_dataset(dataset_id, metadata_url, auth_url, downloader):
    response = downloader.download(metadata_url % dataset_id)
    json = response.json()
    study_desc = json['study_desc']
    title_statement = study_desc['title_statement']
    title = title_statement['title']
    logger.info(f'Creating dataset: {title}')
    study_info = study_desc['study_info']
    data_collection = study_desc['method']['data_collection']
    sources = [x['name'] for x in study_desc['authoring_entity']]
    methodology = list()
    data_kind = study_info.get('data_kind')
    if data_kind is not None:
        methodology.append(f'Kind of Data: {data_kind}  \n')
    unit_analysis = study_info.get('universe')
    if unit_analysis is None:
        unit_analysis = study_info.get('analysis_unit')
    if unit_analysis is not None:
        methodology.append(f'Unit of Analysis: {unit_analysis}  \n')
    sampling = data_collection.get('sampling_procedure')
    if sampling is not None:
        methodology.append(f'Sampling Procedure: {sampling}  \n')
    collection = data_collection.get('coll_mode')
    if collection is not None:
        methodology.append(f'Data Collection Mode: {collection}  \n')
    dataset_name = slugify(title_statement['idno'])
    dataset = Dataset({
        'name': dataset_name,
        'title': title,
        'notes': study_info['abstract'],
        'dataset_source': ', '.join(sources),
        'methodology': 'Other',
        'methodology_other': ''.join(methodology)
    })
    dataset.set_maintainer('ac47b0c8-548b-4c37-a685-7377e75aad55')
    dataset.set_organization('abf4ca86-8e69-40b1-92f7-71509992be88')
    dataset.set_expected_update_frequency('Never')
    dataset.set_subnational(True)
    for nation in study_info['nation']:
        countryiso = nation['abbreviation']
        if not countryiso:
            countryname = nation['name']
            if countryname:
                countryiso, _ = Country.get_iso3_country_code_fuzzy(countryname)
        if countryiso:
            dataset.add_country_location(countryiso)
    tags = list()

    def add_tags(inwords, key):
        for inword in inwords:
            inword = inword[key].strip().lower()
            if ',' in inword:
                words = inword.split(',')
            elif '/' in inword:
                words = inword.split('/')
            else:
                words = [inword]
            newwords = list()
            for innerword in words:
                if 'and' in innerword:
                    newwords.extend(innerword.split(' and '))
                elif '&' in innerword:
                    newwords.extend(innerword.split(' & '))
                elif 'other' in innerword:
                    newwords.extend(innerword.split('other'))
                else:
                    newwords.append(innerword)
            for word in newwords:
                word = word.strip()
                if word:
                    tags.append(word.strip())

    add_tags(study_info['topics'], 'topic')
    add_tags(study_info.get('keywords', list()), 'keyword')
    dataset.add_tags(tags)
    dataset.clean_tags()
    dataset.set_dataset_date(study_info['coll_dates'][0]['start'], study_info['coll_dates'][0]['end'])

    resourcedata = {
        'name': title,
        'description': 'Log in after clicking download link',
        'url': auth_url % dataset_id,
        'format': 'login'
    }
    dataset.add_update_resource(resourcedata)
    return dataset
