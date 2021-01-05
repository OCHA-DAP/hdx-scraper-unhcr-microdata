### Collector for UNHCR's Microdata Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-unhcr-microdata/workflows/build/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-unhcr-microdata/actions?query=workflow%3Abuild) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-unhcr-microdata/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-unhcr-microdata?branch=master)

This script connects to the [UNHCR API](https://microdata.unhcr.org/index.php/api/catalog/) and extracts data dataset by dataset creating them in HDX. The scraper takes around 10 minutes to run and produces no local data (other than state). It runs weekly and resources point to a login form on UNHCR. 


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-unhcr-microdata** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, TEMP_DIR, LOG_FILE_ONLY