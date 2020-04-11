import sys
from pathlib import Path
from sys import argv

import jenkspy
from datetime import datetime, timedelta
import pandas as pd
import yaml
from loguru import logger

from lxml.builder import ElementMaker
from lxml import etree as et


def load_config_file(config_file_path):
    """
    Read config file and necessary paths
    :param config_file_path:
    :return: paths needed from config file
    """
    with open(config_file_path, 'r') as f:
        try:
            config = yaml.load(f, Loader=yaml.FullLoader)
        except yaml.YAMLError as ex:
            logger.warning(ex)

    config['state_github_raw_data'] = Path(config['state_github_raw_data'])
    config['county_github_raw_data'] = Path(config['county_github_raw_data'])
    config['sourceDirectory'] = Path(config['sourceDirectory'])
    config['state_missing_geo'] = Path(config['state_missing_geo'])
    config['geo_acs18_5yr_raw_data'] = Path(config['geo_acs18_5yr_raw_data'])
    config['config'] = Path(config['configDirectory'])

    return config


def calculate_brackets(last_file_data, var_prefix):
    breaks = {}
    for variable_name, values in last_file_data.iteritems():
        if var_prefix in variable_name:
            try:
                breaks[variable_name[len(var_prefix) + 1:]] = jenkspy.jenks_breaks(values, nb_class=11)
            except ValueError as e:
                logger.warning(f'Cannot create a cupoint file for variable {variable_name}, error: {e}')
                continue
    return breaks


def generate_xml_with_cutpoints(brackets, output_path):
    """
    Naming convention project name + name in title case e.g. COVID19Cases, COVID19Deaths
    Example output xml:

    <CategoryFilters name="COVID19Cases" valueFormat="Number">
        <FilterSet>
            <Filter from="" to="271" />
            <Filter from="271" to="479" />
            <Filter from="479" to="838" />
            <Filter from="838" to="1066" />
            <Filter from="1066" to="" />
 	    </FilterSet>
    </CategoryFilters>

    :param brackets: 
    :param output_path: 
    :return: 
    """

    for file_name, values in brackets.items():
        xml_structure = ElementMaker()

        filter_nodes = []
        for i, value in enumerate(values):
            e = ElementMaker()

            start = str(int(value)) if value != 0 else ''
            end = str(int(values[i + 1])) if i+1 < len(values) else ''

            filter_nodes.append(e.Filter(**{'from': start, 'to': end}))

        page = xml_structure.CategoryFilters(
            e.FilterSet(
                *filter_nodes
            ),
            name="COVID19"+file_name.title(),
            valueFormat="Number"
        )

        tree = et.ElementTree(page)
        tree.write(output_path + '/' + file_name + '.xml', pretty_print=True)


def generate_cutpoints_for_dataset(config_file_path):
    config = load_config_file(config_file_path)

    path_to_preprocessed_files = config['sourceDirectory']

    last_file_content = None
    current_date = datetime.today()
    while last_file_content is None:
        try:
            last_date_file_stamp = str(current_date.strftime('%b_%d_%Y')).replace('_0', '_')
            last_file_content = pd.read_csv(str(path_to_preprocessed_files) + '\\' + last_date_file_stamp + '.csv').dropna()
        except FileNotFoundError:
            current_date = current_date - timedelta(days=1)

    brackets = calculate_brackets(last_file_content, last_date_file_stamp)
    generate_xml_with_cutpoints(brackets, config['metadataOutputPath'])


if __name__ == '__main__':
    try:
        config_file_path = argv[1]
    except IndexError:
        logger.error('Add path to config file if you want to run this script in standalone mode! Exiting ...')
        sys.exit()

    generate_cutpoints_for_dataset(config_file_path)
