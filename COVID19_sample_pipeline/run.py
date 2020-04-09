"""
This will run the whole pipeline: preprocessing, writing to db and creating xml metadata

v1.00
"""
from pathlib import Path
from sys import argv, exit

import scripts.run_config_check as config_check
import scripts.preprocess_covid as preprocess_covid
import scripts.process_and_load as process_and_load
import scripts.create_metadata_file as create_metadata_file
import scripts.compute_additional_columns as compute_aditional_columns
import scripts.get_existing_guids_from_xml as existing_xml_guids
import scripts.generate_cutpoints as generate_cutpoints
import scripts.get_data as get_data


from loguru import logger


def main(processing_steps, config_path):
    run_step = {
        'config_check': config_check.config_file_check,
        'get_data': get_data.download_data_nyt,
        'preprocess_covid': preprocess_covid.preprocess_output,
        'compute_additional_columns': compute_aditional_columns.compute_new_columns,
        'process_and_load_data': process_and_load.process_and_load_data,
        'create_metadata_file_structure': create_metadata_file.create_metadata_file,
        'existing_xml_guids': existing_xml_guids.get_existing_guids,
        'generate_cutpoints': generate_cutpoints.generate_cutpoints_for_dataset,
    }

    for step, params in processing_steps.items():
        logger.info(f'Running step {step}'.center(20, '🐍'))
        run_step[step](config_path)

    logger.info('Great success!!!')


if __name__ == '__main__':

    if len(argv) != 2 or not Path(argv[1]).is_file():
        logger.error('Cannot find config file, run this with config file as the first parameter!')
        # TODO create a proper CLI interface
        exit()
    full_config_path = argv[1]

    steps = {
        'config_check': {},
        'get_data': {},
        'preprocess_covid': {},
        'compute_additional_columns': {},
        # 'process_and_load_data': {},
        # 'create_metadata_file_structure': {},
        # 'existing_xml_guids': {},
        # 'generate_cutpoints': {},
    }

    main(steps, full_config_path)
