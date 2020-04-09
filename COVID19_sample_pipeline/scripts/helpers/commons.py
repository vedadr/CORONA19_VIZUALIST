"""
Smaller functions used by many scripts
"""
import csv
from pathlib import Path

import yaml
from loguru import logger


def get_config(config_file):
    with open(config_file, 'r') as conf:
        config = yaml.load(conf, Loader=yaml.FullLoader)
    return config


def create_acronym(full_string):
    """
    Create acronym from a multi word full_string, just capitalize it if one word is in full_string.
    :param full_string: String to create acronym from
    :return:
    """
    if type(full_string) is int:
        return str(full_string)

    chars_for_removal = [
        '(', ')', '&', '/', ',', '.',
        '\\', '\'', '&', '%', '#',
    ]

    for ch in chars_for_removal:
        if ch in full_string:
            full_string = full_string.replace(ch, ' ')
    full_string = full_string.strip()
    while '  ' in full_string:
        full_string = full_string.replace('  ', ' ')
    if ' ' in full_string:
        blank_pos = [
            i for i, letter in enumerate(
                full_string,
            ) if letter == ' '
        ]
        acronym = full_string[0] + \
            ''.join([full_string[i + 1] for i in blank_pos]).upper()
    else:
        acronym = full_string.upper()
    return acronym


def check_data_type(datum):
    """
    This function checks type of value because pymssql cannot distinguish int from float
    :param datum: Value to be checked
    :return:
    """
    if datum == 'int':
        return 3
    elif datum == 'bigint':
        return 5
    elif datum == 'float':
        return 2
    elif 'char' in datum or datum == 'text':
        return 1
    elif datum is None:
        return 9
    else:
        logger.info(
            "Undefined data type in source data for: " +
            datum,
        )  # if nothing then string
        return None


def get_metadata_from_input_file(definition_path) -> dict:
    """
    Open file and read variable names and its description. Files must be in format: variable_id, description
    :param definition_path: Full path to the file
    :return: Dictionary with variable id as key and its definition as a dictionary i.e.
    {'var1': {'code': '','title': '','indent': '','category': '','bubble_size_hint': '','method' :'','color_palettes': ''}}
    """
    file_content_dict = {}
    path = Path(definition_path)

    try: 
        with open(path, encoding="utf-8") as file:
            reader = csv.reader(file)
            for index, line in enumerate(list(reader)):
                if index == 0:
                    header = line
                else:
                    file_content_dict[line[0]] = {
                        attrib: line[i]
                        for i, attrib in enumerate(header)
                    }    

    except UnicodeEncodeError:
        with open(path, encoding='utf-8') as file:
            reader = csv.reader(file)
            for index, line in enumerate(list(reader)):
                if index == 0:
                    header = line
                else:
                    file_content_dict[line[0]] = {
                        attrib: line[i]
                        for i, attrib in enumerate(header)
                    }

    return file_content_dict
