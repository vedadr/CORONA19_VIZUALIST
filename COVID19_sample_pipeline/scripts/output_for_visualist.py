from datetime import datetime
from sys import argv

import pandas as pd
from pathlib import Path

from loguru import logger

from .preprocess_covid import load_config_file


def prepare_and_write_output(config_path):
    config = load_config_file(config_path)
    preprocessed_files_location = Path(config['sourceDirectory'])

    data_container = []
    for file in preprocessed_files_location.iterdir():
        date_stamp = file.name[:-4]

        single_day_data = pd.read_csv(str(file), dtype={'GeoID': str})

        new_column_names = [column.replace(date_stamp + '_', '') for column in single_day_data.columns]
        single_day_data.columns = new_column_names

        single_day_data['date'] = datetime.strptime(date_stamp, '%b_%d_%Y').strftime('%Y-%m-%d')

        data_container.append(single_day_data)

    data_container = pd.concat(data_container).replace({'SUMLEV': {'SL010': 'us', 'SL040': 'states', 'SL050': 'counties'}})

    # export data one geo level per CSV
    for geo_level in data_container['SUMLEV'].drop_duplicates():
        data_container[data_container.SUMLEV == geo_level].to_csv(Path.joinpath(preprocessed_files_location, f'{geo_level}_vizualist.csv'), index=False)


if __name__ == '__main__':
    try:
        path_to_config = argv[1]
        prepare_and_write_output(path_to_config)
    except IndexError:
        logger.info('Check your parameters when running this script in standalone mode!')
