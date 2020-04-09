from io import StringIO
from sys import argv

import pandas as pd
import requests
from loguru import logger

from scripts.preprocess_covid import load_config_file


def download_data_nyt(config_file_path):
    """
    Download covid data by date
    :param config_path: config
    :return: raw data to csv written
    """

    config = load_config_file(config_file_path)

    cty_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    state_url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv'

    try:
        logger.info('Calling cty url:- ' + cty_url)
        byte_covid_cty_data = str(requests.get(cty_url).content, 'utf-8')
        covid_data_convert_cty_from_byte = StringIO(byte_covid_cty_data)
        covid_data_cty = pd.read_csv(covid_data_convert_cty_from_byte, converters={'fips': str})
        covid_data_cty.to_csv(config['county_github_raw_data'], index=False)

    except Exception as e:
        logger.info('Exception happened. EXCEPTION Message is ' + str(e))

    try:
        logger.info('Calling state url:- ' + state_url)
        byte_covid_state_data = str(requests.get(state_url).content, 'utf-8')
        covid_data_convert_state_from_byte = StringIO(byte_covid_state_data)
        covid_data_state = pd.read_csv(covid_data_convert_state_from_byte, converters={'fips': str})
        covid_data_state.to_csv(config['state_github_raw_data'], index=False)

    except Exception as e:
        logger.info('Exception happened. EXCEPTION Message is ' + str(e))


if __name__ == '__main__':
    try:
        path_to_config = argv[1]
        download_data_nyt(path_to_config)
    except IndexError:
        logger.info('Check your parameters when running this script in standalone mode!')

