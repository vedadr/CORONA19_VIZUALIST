from pathlib import Path
from sys import argv
from datetime import datetime, timedelta
import pandas as pd

from loguru import logger

import numpy as np

from .preprocess_covid import load_config_file


def calculate_change(current_data_holder, file, type, interval):
    current_date_stamp = file.name[:-4]
    current_date = datetime.strptime(current_date_stamp.replace('_', ' '), '%b %d %Y')

    previous_day = current_date - timedelta(days=interval)
    previous_day_stamp = datetime.strftime(previous_day, '%b_%d_%Y').replace('_0', '_')

    try:
        previous_date_content = pd.read_csv(str(file.parent) + '\\' + previous_day_stamp + '.csv', dtype={'GeoID': str},
                                            encoding='utf-8')
    except FileNotFoundError:
        return current_data_holder[current_date_stamp + '_' + type]

    # since the number of the counties changed on daily level, we had to merge current with previous datasets
    # to make sure that proper counties are being subtracted
    temp_diff_holder = pd.merge(current_data_holder, previous_date_content, on='GeoID', how='left')
    return temp_diff_holder[current_date_stamp + '_' + type] - temp_diff_holder[previous_day_stamp + '_' + type]


def calculate_cumulative_percent_change(current_data_holder, file, type, interval):
    current_date_stamp = file.name[:-4]
    current_date = datetime.strptime(current_date_stamp.replace('_', ' '), '%b %d %Y')

    previous_day = current_date - timedelta(days=interval)
    previous_period_stamp = datetime.strftime(previous_day, '%b_%d_%Y').replace('_0', '_')

    try:
        previous_date_content = pd.read_csv(str(file.parent) + '\\' + previous_period_stamp + '.csv', dtype={'GeoID': str},
                                            encoding='utf-8')
    except FileNotFoundError:
        return current_data_holder[current_date_stamp + '_' + type]

    # since the number of the counties changed on daily level, we had to merge current with previous datasets
    # to make sure that proper counties are being subtracted
    temp_diff_holder = pd.merge(current_data_holder, previous_date_content, on='GeoID', how='left')
    return (temp_diff_holder[current_date_stamp + '_' + type] / temp_diff_holder[previous_period_stamp + '_' + type] * 100 - 100).round(2)


def calculate_percent_change_period_over_period(current_data_holder, file, type, interval):
    current_date_stamp = file.name[:-4]
    current_date = datetime.strptime(current_date_stamp.replace('_', ' '), '%b %d %Y')

    if interval == 'day':
        offset_value = {'days': 1}
        offset_value_prev = {'days': 2}
    elif interval == 'week':
        offset_value = {'days': 7}
        offset_value_prev = {'days': 14}
    else:
        offset_value = {'days': 30}  # TODO find more accurate solution for this
        offset_value_prev = {'days': 60}

    previous_day = current_date - timedelta(**offset_value)
    previous_date_stamp = datetime.strftime(previous_day, '%b_%d_%Y').replace('_0', '_')

    day_before_previous_day = current_date - timedelta(**offset_value_prev)
    date_before_previous_day_stamp = datetime.strftime(day_before_previous_day, '%b_%d_%Y').replace('_0', '_')

    try:
        previous_date_content = pd.read_csv(str(file.parent) + '\\' + previous_date_stamp + '.csv', dtype={'GeoID': str})
        date_before_previous_date_content = pd.read_csv(str(file.parent) + '\\' + date_before_previous_day_stamp + '.csv', dtype={'GeoID': str})
    except FileNotFoundError:
        return pd.Series(np.arange(current_data_holder.shape[0]))

    # since the number of the counties changed on daily level, we had to merge current with previous datasets
    # to make sure that proper counties are being used for calculation
    temp_diff_holder = pd.merge(current_data_holder, previous_date_content, on='GeoID', how='left').merge(date_before_previous_date_content, on='GeoID', how='left')

    return (((temp_diff_holder[current_date_stamp + '_' + type]
            - temp_diff_holder[previous_date_stamp + '_' + type]) /\
           (temp_diff_holder[previous_date_stamp + '_' + type]
            - temp_diff_holder[date_before_previous_day_stamp + '_' + type])).replace([np.inf, -np.inf], np.nan) * 100 - 100).round(2)


def calculate_rate_per_100k(data_table, file, type, acs_data_location):
    acs_data = pd.concat([pd.read_csv(Path.joinpath(acs_data_location, 'ACS2018_SL010.csv'), dtype={'Geo_FIPS': str}, encoding='latin-1'),
                         pd.read_csv(Path.joinpath(acs_data_location, 'ACS2018_SL040.csv'), dtype={'Geo_FIPS': str}, encoding='latin-1'),
                         pd.read_csv(Path(acs_data_location, 'ACS2018_SL050.csv'), dtype={'Geo_FIPS': str}, encoding='latin-1')])
    acs_data = acs_data.rename({'Geo_FIPS': 'GeoID'}, axis=1)
    temp_diff_holder = pd.merge(data_table, acs_data[['GeoID', 'SE_A00001_001']], on='GeoID', how='left')

    return (temp_diff_holder[file.name[:-4] + '_' + type] / temp_diff_holder['SE_A00001_001'] * 100000).round(2)


def calculate_growth_rate(current_data_holder, file, type):
    current_date_stamp = file.name[:-4]
    current_date = datetime.strptime(current_date_stamp.replace('_', ' '), '%b %d %Y')

    previous_day = current_date - timedelta(days=1)
    previous_day_stamp = datetime.strftime(previous_day, '%b_%d_%Y').replace('_0', '_')

    try:
        previous_date_content = pd.read_csv(str(file.parent) + '\\' + previous_day_stamp + '.csv', dtype={'GeoID': str}, encoding='utf-8')
    except FileNotFoundError:
        return pd.Series(np.arange(current_data_holder.shape[0]))

    # since the number of the counties changed on daily level, we had to merge current with previous datasets
    # to make sure that proper counties are being used for calculation
    temp_diff_holder = pd.merge(current_data_holder, previous_date_content, on='GeoID', how='left')

    return ((temp_diff_holder[current_date_stamp + '_' + type] - temp_diff_holder[previous_day_stamp + '_' + type]) / temp_diff_holder[previous_day_stamp + '_' + type] * 100)\
        .replace([np.inf, -np.inf], np.nan)\
        .round(1)


def calculate_percent_pop(data_table, file, type, acs_data_location):
    acs_data = pd.concat([pd.read_csv(Path.joinpath(acs_data_location, 'ACS2018_SL010.csv'), dtype={'Geo_FIPS': str}, encoding='latin-1'),
                          pd.read_csv(Path.joinpath(acs_data_location, 'ACS2018_SL040.csv'), dtype={'Geo_FIPS': str}, encoding='latin-1'),
                          pd.read_csv(Path(acs_data_location, 'ACS2018_SL050.csv'), dtype={'Geo_FIPS': str}, encoding='latin-1')])
    acs_data = acs_data.rename({'Geo_FIPS': 'GeoID'}, axis=1)
    temp_diff_holder = pd.merge(data_table, acs_data[['GeoID', 'SE_A00001_001']], on='GeoID', how='left')

    return (temp_diff_holder[file.name[:-4] + '_' + type] / temp_diff_holder['SE_A00001_001'] * 100).round(2)


def calculate_doubling_rate(data_holder, file, type):
    if type == 'cases':
            dbl_rate = 70 / data_holder[file.name[:-4] + '_growth_rate_confirmed_cases'].round()
    else:
            dbl_rate = 70 / data_holder[file.name[:-4] + '_growth_rate_deaths'].round()

    return dbl_rate.replace([np.inf, -np.inf], np.nan).round(1)


def compute_new_columns(config_file_path):
    pd.options.display.float_format = '${:,.2f}'.format

    config = load_config_file(config_file_path)

    path_to_preprocessed_files = config['sourceDirectory']
    for file in Path(path_to_preprocessed_files).iterdir():
        logger.info(f'Computing for {file.name}')

        data_timestamp = file.name[:-4]

        daily_data = pd.read_csv(str(file), dtype={'GeoID': str}, encoding='utf-8')

        try:
            daily_data = daily_data\
                .assign(**{data_timestamp+'_daily_change_confirmed_cases': lambda x: calculate_change(x, file, 'cases', 1)}) \
                .assign(**{data_timestamp + '_growth_rate_confirmed_cases': lambda x: calculate_growth_rate(x, file, 'cases')}) \
                .assign(**{data_timestamp + '_percent_pop_confirmed_cases': lambda x: calculate_percent_pop(x, file, 'cases', config['acs_data_location'])}) \
                .assign(**{data_timestamp + '_daily_change_in_growth_confirmed_cases_percent': lambda x: calculate_percent_change_period_over_period(x, file, 'cases', 'day')}) \
                .assign(**{data_timestamp + '_daily_change_confirmed_cases_doubling_time': lambda x: calculate_doubling_rate(x, file, 'cases')}) \
                .assign(**{data_timestamp + '_confirmed_rate_per_100k': lambda x: calculate_rate_per_100k(x, file, 'cases', config['acs_data_location'])}) \
            \
                .assign(**{data_timestamp+'_daily_change_deaths': lambda x: calculate_change(x, file, 'deaths', 1)}) \
                .assign(**{data_timestamp + '_growth_rate_deaths': lambda x: calculate_growth_rate(x, file, 'deaths')}) \
                .assign(**{data_timestamp + '_percent_pop_deaths': lambda x: calculate_percent_pop(x, file, 'deaths', config['acs_data_location'])}) \
                .assign(**{data_timestamp + '_daily_change_in_growth_deaths_percent': lambda x: calculate_percent_change_period_over_period(x, file, 'deaths', 'day')}) \
                .assign(**{data_timestamp + '_daily_change_deaths_doubling_time': lambda x: calculate_doubling_rate(x, file, 'deaths')}) \
                .assign(**{data_timestamp + '_deaths_rate_per_100k': lambda x: calculate_rate_per_100k(x, file, 'deaths', config['acs_data_location'])}) \
            \
                .assign(**{data_timestamp + '_weekly_change_confirmed_cases': lambda x: calculate_change(x, file, 'cases', 7)}) \
                .assign(**{data_timestamp + '_weekly_change_in_growth_confirmed_cases_percent': lambda x: calculate_percent_change_period_over_period(x, file, 'cases', 'week')}) \
                .assign(**{data_timestamp + '_weekly_cumulative_change_in_confirmed_cases_percent': lambda x: calculate_cumulative_percent_change(x, file, 'cases', 7)}) \
 \
                .assign(**{data_timestamp + '_weekly_change_deaths': lambda x: calculate_change(x, file, 'deaths', 7)}) \
                .assign(**{data_timestamp + '_weekly_change_in_growth_deaths_percent': lambda x: calculate_percent_change_period_over_period(x, file, 'deaths', 'week')}) \
                .assign(**{data_timestamp + '_weekly_cumulative_change_in_deaths_percent': lambda x: calculate_cumulative_percent_change(x, file, 'deaths', 7)}) \
 \
                .assign(**{data_timestamp + '_monthly_change_confirmed_cases': lambda x: calculate_change(x, file, 'cases', 30)}) \
                .assign(**{data_timestamp + '_monthly_change_in_growth_confirmed_cases_percent': lambda x: calculate_percent_change_period_over_period(x, file, 'cases', 'month')}) \
                .assign(**{data_timestamp + '_monthly_cumulative_change_in_confirmed_cases_percent': lambda x: calculate_cumulative_percent_change(x, file, 'cases', 30)}) \
 \
                .assign(**{data_timestamp + '_monthly_change_deaths': lambda x: calculate_change(x, file, 'deaths', 30)}) \
                .assign(**{data_timestamp + '_monthly_change_in_growth_deaths_percent': lambda x: calculate_percent_change_period_over_period(x, file, 'deaths', 'month')}) \
                .assign(**{data_timestamp + '_monthly_cumulative_change_in_deaths_percent': lambda x: calculate_cumulative_percent_change(x, file, 'deaths', 30)}) \
 \
                .fillna({data_timestamp+'_daily_change_confirmed_cases': 0, data_timestamp+'_daily_change_deaths': 0,
                         data_timestamp+'_weekly_change_confirmed_cases': 0, data_timestamp+'_weekly_change_deaths': 0,
                         data_timestamp+'_monthly_change_confirmed_cases': 0, data_timestamp+'_monthly_change_deaths': 0,
                         data_timestamp+'_growth_rate_confirmed_cases': 0, data_timestamp+'_growth_rate_deaths': 0,
                         data_timestamp+'_daily_change_confirmed_cases_doubling_time': 0,
                         data_timestamp+'_daily_change_deaths_doubling_time': 0
                         })\
                .astype({data_timestamp+'_daily_change_confirmed_cases': 'int32',
                         data_timestamp+'_daily_change_deaths': 'int32',
                         data_timestamp+'_weekly_change_confirmed_cases': 'int32',
                         data_timestamp + '_weekly_cumulative_change_in_confirmed_cases_percent': 'float',
                         data_timestamp + '_weekly_cumulative_change_in_deaths_percent': 'float',
                         data_timestamp+'_weekly_change_deaths': 'int32',
                         data_timestamp+'_monthly_change_confirmed_cases': 'int32',
                         data_timestamp+'_monthly_change_deaths': 'int32',
                         data_timestamp + '_monthly_cumulative_change_in_confirmed_cases_percent': 'float',
                         data_timestamp + '_monthly_cumulative_change_in_deaths_percent': 'float',
                         data_timestamp+'_growth_rate_confirmed_cases': 'float',
                         data_timestamp+'_growth_rate_deaths': 'float',
                         data_timestamp+'_daily_change_confirmed_cases_doubling_time': 'float',
                         data_timestamp+'_daily_change_deaths_doubling_time': 'float'
                         })

            daily_data.to_csv(str(file), index=False)

        except ValueError as e:
            logger.warning('Not a file with data!?')
            logger.warning(e)


if __name__ == '__main__':
    try:
        path_to_config = argv[1]
        compute_new_columns(path_to_config)
    except IndexError:
        logger.info('Check your parameters when running this script in standalone mode!')
