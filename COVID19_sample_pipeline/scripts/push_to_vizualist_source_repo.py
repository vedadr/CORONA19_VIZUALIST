from pathlib import Path
from sys import argv

import shutil

import git
from loguru import logger

from scripts.preprocess_covid import load_config_file


def push_content(config_path):
    config = load_config_file(config_path)
    preprocessed_files_location = Path(config['sourceDirectory'])

    repo_corona_path = 'C:\projects\CORONA19_VISUALIST'

    # copy to vizualist repo
    shutil.copy(Path(preprocessed_files_location, f'us_vizualist.csv'), repo_corona_path)
    shutil.copy(Path(preprocessed_files_location, f'states_vizualist.csv'), repo_corona_path)
    shutil.copy(Path(preprocessed_files_location, f'counties_vizualist.csv'), repo_corona_path)


    repo = git.Repo(repo_corona_path)
    o = repo.remotes.origin
    o.push()


if __name__ == '__main__':
    try:
        path_to_config = argv[1]
        push_content(path_to_config)
    except IndexError:
        logger.info('Check your parameters when running this script in standalone mode!')
