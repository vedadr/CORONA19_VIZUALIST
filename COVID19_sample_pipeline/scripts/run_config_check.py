from pathlib import Path
from loguru import logger
import datetime
from scripts.helpers.commons import get_config
import sys
from scripts.helpers.logger_aditional_info import show_step_info


@show_step_info
def config_file_check(config_path):
    errors = []
    warnings = []

    if not Path(config_path).exists():
        logger.info("Error: Config file doesn't exist on selected location: " + config_path + " !")
        return False

    config = get_config(config_path)
    if len(config['projectName']) == 0 or 'projectName' not in config.keys():
        errors.append('Error: Project name not set properly in config file!')
    if len(config['projectId']) == 0 or 'projectId' not in config.keys():
        errors.append('Error: Project id not set properly in config file!')
    if len(config['serverName']) == 0 or 'serverName' not in config.keys():
        errors.append('Error: Server name not set properly in config file!')
    if len(config['databaseName']) == 0 or 'databaseName' not in config.keys():
        errors.append('Error: Database name not set properly in config file!')
    if len(config['connectionCredentialsName']) == 0 or 'connectionCredentialsName' not in config.keys():
        errors.append('Error:  not set properly in config file!')
    if len(config['projectYear']) != 4 or 'projectYear' not in config.keys():
        errors.append('Error: Project year not set properly in config file!')
    if int(config['projectYear']) > datetime.datetime.now().year:
        warnings.append('Warning: Project year is set in future.')
    if not Path(config['variableDefinitionsPath']).exists() or not Path(config['variableDefinitionsPath']).is_file():
        errors.append('Error: Something is wrong with variable info location!')
    if not Path(config['metadataOutputPath']).exists():
        errors.append('Error: Metadata Output folder does not exits!')
    if not Path(config['tableDefinitionsPath']).exists() or not Path(config['tableDefinitionsPath']).is_file():
        errors.append('Error: Something is wrong with table definition file!')
    if not Path(config['sourceDirectory']).exists() or Path(config['sourceDirectory']).is_file():
        errors.append('Error: Source directory does not exists or empty!')
    if not Path(config['configDirectory']).exists() or Path(config['configDirectory']).is_file():
        errors.append('Error: Source directory does not exists or empty!')

    for warn in warnings:
        logger.info(warn)

    if len(errors) == 0:
        return True

    else:
        for err in errors:
            logger.info(err)
        return False


if __name__ == '__main__':
    if not config_file_check('../config.yml'):
        logger.info('Please check errors before processing!')
        sys.exit(1)
