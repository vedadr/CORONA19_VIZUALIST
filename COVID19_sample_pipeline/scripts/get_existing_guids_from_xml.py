from lxml import etree as et
import time
import datetime
import json
import yaml
from sys import argv
from loguru import logger

# TODO check do we need this!
# start = time.time()
# print(datetime.datetime.now())
# now = datetime.datetime.now()


def load_config_file(config_file_path):
    """
    Read config file and necessary paths
    :param config_file_path:
    :return: paths needed from config file
    """
    with open(config_file_path, 'r') as f:
        try:
            conf = yaml.load(f, Loader=yaml.FullLoader)
        except yaml.YAMLError as ex:
            logger.warning(ex)

    return conf


def get_existing_guids(config_path):

    config = load_config_file(config_path)

    tree = et.parse(config['metadataOutputPath'] + config['metadataFileName'])
    root = tree.getroot()
    var_guids_old = root.findall("SurveyDatasets/SurveyDataset/tables/table/variable")
    org_vars_guid = {}

    for variable in var_guids_old:
        org_vars_guid[variable.attrib['name']] = variable.attrib['GUID']

    with open(config['metadataOutputPath'] + 'org_vars_guid.json', 'w') as f:
        json.dump(org_vars_guid, f)


if __name__ == '__main__':
    config_path = argv[1]
    config = load_config_file(config_path)
    get_existing_guids(config)



