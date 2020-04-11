import numpy as np
import pandas as pd
import calendar
from loguru import logger
import os
import yaml
from pathlib import Path
from sys import argv


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
    config['acs_data_location'] = Path(config['acs_data_location'])

    return config


def read_data(st_path, cty_path):
    """
    Read csv downloaded data for state and county
    :param st_path: path where state data is downloaded
    :param cty_path: path where county data is downloaded
    :return:
    """
    state_covid_data = pd.read_csv(st_path, converters={'fips': str}, encoding='utf-8')
    county_covid_data = pd.read_csv(cty_path, converters={'fips': str}, encoding='utf-8')

    return state_covid_data, county_covid_data


def get_cty_geo_columns(county_covid, conf) -> pd.DataFrame:
    """
    Fix county geo columns
    :param county_covid: original raw github csv data file
    :param conf: config file
    :return: dataframe with geo columns in place
    """
    state_geo_fips_name = pd.read_csv(conf['state_missing_geo'], converters={'SL040_FIPS': str})
    logger.warning('There are ' + str(len(county_covid.loc[county_covid.fips == ''])) + ' with missing fips.')
    county_covid = pd.merge(county_covid, state_geo_fips_name, left_on=county_covid.state,
                            right_on=state_geo_fips_name.SL040_NAME, how='left')
    county_covid = county_covid.assign(
            fips=np.where(county_covid['county'] == 'New York City', county_covid['SL040_FIPS'] + '888', county_covid['fips']))
    county_covid = county_covid.assign(
            fips=np.where(county_covid['county'] == 'Kansas City', county_covid['SL040_FIPS'] + '888', county_covid['fips']))
    county_covid = county_covid.assign(
            fips=np.where(county_covid['county'] == 'Unknown', county_covid['SL040_FIPS'] + '999', county_covid['fips']))
    county_covid['SUMLEV'] = 'SL050'
    county_covid['NAME'] = county_covid['county'] + ' County'
    county_covid['TYPE'] = 'County'
    county_covid['QName'] = county_covid['NAME'] + ', ' + county_covid['state']
    county_covid.drop(['key_0'], axis=1, inplace=True)

    county_covid.rename(columns={'fips': 'GeoID'}, inplace=True)
    county_covid.drop(['county', 'state', 'SL040_FIPS', 'SL040_NAME'], axis=1, inplace=True)

    return county_covid


def get_state_geo_columns(st_covid) -> pd.DataFrame:
    """
    Fix state geo columns
    :param st_covid: original raw github csv data file
    :return: dataframe with geo columns in place
    """
    st_covid['SUMLEV'] = 'SL040'
    st_covid['NAME'] = st_covid['state']
    st_covid['TYPE'] = 'State'
    st_covid.rename(columns={'fips': 'GeoID'}, inplace=True)
    st_covid['QName'] = st_covid['NAME']
    st_covid.drop(['state'], axis=1, inplace=True)

    return st_covid


def fix_date_time(data_covid):
    """
    Fix column where date and get nice day, year, mont columns to pivot it easily
    :param data_covid: dataframe where this function will be applied
    """
    data_covid['Year'] = pd.DatetimeIndex(data_covid['date']).year.astype(str)
    data_covid['Month'] = pd.DatetimeIndex(data_covid['date']).month.astype(int)
    data_covid['Month'] = data_covid['Month'].apply(lambda x: calendar.month_abbr[x])
    data_covid['Day'] = pd.DatetimeIndex(data_covid['date']).day.astype(str)
    data_covid['Date'] = data_covid['Month'] + '_' + data_covid[
        'Day'] + '_' + data_covid['Year']
    data_covid.drop(['date', 'Year', 'Month', 'Day'], axis=1, inplace=True)


def append_st_cty(cty_cov, st_cov) -> pd.DataFrame:
    """
    Append state and county data
    :param cty_cov: county covid data
    :param st_cov: state covid data
    :return: cty_states_covid, date fixed, appended state and county
    """
    fix_date_time(cty_cov)
    fix_date_time(st_cov)

    cty_states_covid = st_cov.append(cty_cov)

    return cty_states_covid


def add_us_cases(covid_data_state_county) -> pd.DataFrame:
    """
    Aggregate US
    :param covid_data_state_county: state county covid data
    :return: us aggregated
    """
    geo_cols = ['GeoID', 'SUMLEV', 'Date']
    data_cols = [col for col in covid_data_state_county.columns if col not in geo_cols]
    get_states_only = covid_data_state_county.loc[covid_data_state_county['SUMLEV'] == 'SL040']
    us_grouped = get_states_only.groupby('Date')[data_cols].sum()
    us_grouped['GeoID'] = '00'
    us_grouped['SUMLEV'] = 'SL010'
    us_grouped['TYPE'] = 'Nation'
    us_grouped['NAME'] = 'United States'
    us_grouped['QName'] = us_grouped['NAME']
    us_grouped['Date'] = us_grouped.index
    us_grouped.reset_index(drop=True, inplace=True)

    return us_grouped


def append_us_to_st_cty(us_covid_cases, st_cty_cases) -> pd.DataFrame:
    """
    Append us to state and county
    :param us_covid_cases: aggregated us cases per date
    :param st_cty_cases: st county cases
    :return: appended us to state and county
    """
    all_data = us_covid_cases.append(st_cty_cases, sort=False)
    all_data.reset_index(drop=True, inplace=True)

    return all_data


def write_name_qname_fips(all_data):
    """
    Write csv file with name, qname, fips for geobuffer. This will be written one time. If there is a need to run it
    again, uncomment it in the main
    :param all_data: all data with geos columns
    :return:
    """
    geo_data = all_data[['GeoID', 'NAME', 'QName']]
    geo_data.drop_duplicates(subset=['GeoID'], inplace=True)
    geo_data.to_csv(config.config_path + 'geo_info.csv', index=False)


def return_each_date(date) -> list:
    """
    Get each date into list in order to pivot and write csv per date
    :param date: string
    :return: list od dates from dataframe
    """
    return sorted(set(list(date['Date'])))


def pivot_and_write_table(pivot_all_data, columns_covid_cases, conf):
    """
    Pivot and write data to csv
    :param pivot_all_data: all data for all geos
    :param columns_covid_cases: data columns
    :param conf: all data for all geos
    :return:
    """
    pivot_all_data.drop(['QName'], axis=1, inplace=True)

    covid_data = pivot_all_data[['GeoID', 'SUMLEV', 'Date'] + columns_covid_cases]

    geo_extract = covid_data[['SUMLEV', 'GeoID']].drop_duplicates()

    list_of_dates = return_each_date(covid_data)

    for each_date in list_of_dates:
        data_per_date = covid_data[covid_data['Date'] == each_date]

        data_per_date = pd.pivot_table(data_per_date, values=columns_covid_cases, index='GeoID', columns='Date')

        data_per_date.columns = pd.Index([mult_ind[1] + '_' + str(mult_ind[0])
                                          for mult_ind in data_per_date.columns.tolist()])

        data_per_date['GeoID'] = data_per_date.index

        data_per_date.reset_index(drop=True, inplace=True)

        data_per_date['Date'] = '_'.join(data_per_date.columns[0].split('_')[:-1])

        pivot_covid_data = pd.merge(data_per_date, geo_extract, how='left', on='GeoID')

        pivot_covid_data = pivot_covid_data.sort_values(by='SUMLEV')

        pivot_covid_data.reset_index(drop=True, inplace=True)

        date = pivot_covid_data.iloc[0]['Date']

        pivot_covid_data.to_csv(str(conf['sourceDirectory']) + '\\' + date + '.csv', index=False)


def write_geo_file(acs18_5_yr_geo_file, all_data, config):
    """
    Write all geos csv file
    :param acs18_5_yr_geo_file: geo data from acs 2018 5 yr
    :param all_data: all data with geos columns
    :return:
    """
    all_data = all_data[['GeoID', 'NAME', 'SUMLEV', 'TYPE']]
    all_data.drop_duplicates(inplace=True)
    geo_data = pd.read_csv(acs18_5_yr_geo_file, encoding='windows-1252', converters={'GeoID': str})
    geo_data = pd.concat([geo_data, all_data]).drop_duplicates(keep='first')
    geo_data.drop_duplicates(subset=['GeoID', 'SUMLEV'], inplace=True)
    geo_data.to_csv(str(config['configDirectory']) + 'all_geographies.csv', index=False)


def files_list(config):
    """
    Writing files list
    :return:
    """

    column_list = ['file_name', 'title', 'title_wrapped', 'category', 'filter_rule', 'visible_maps', 'visible_reports',
                   'dollar_year', 'source', 'sub_dataset']

    table_desc = pd.DataFrame(columns=column_list)

    file_list = []

    for dirpath, _, filenames in os.walk(str(config['sourceDirectory']) + '\\'):
        for item in filenames:
            file_list.append(item)

    table_desc.loc[:, 'file_name'] = [f for f in file_list]

    table_desc["Month"] = pd.to_datetime(table_desc.file_name.str[:3], format='%b', errors='coerce').dt.month
    table_desc["Day"] = table_desc.file_name.str[4:6]
    table_desc["Day"] = table_desc["Day"].str.replace('_', '')
    table_desc["Day"] = table_desc["Day"].astype(int)
    table_desc = table_desc.sort_values(by=["Month", "Day"])

    table_desc.drop(["Month", "Day"], axis=1, inplace=True)
    table_desc.to_csv(str(config['config']) + '\\table_definitions.csv', header=True, index=False,
                      mode='w', encoding='utf-8')


def variable_description(covid_data, config):
    """
    Variable Description file
    :return:
    """

    all_vars_list = []

    for each_date in list(covid_data['Date']):
    # var name, var label, formatting, color palette, agg method, agg string, buuble size hint, filter rule
        variable_value_desc = [[each_date + '_cases', 'Cumulative Number of Confirmed Cases on ' + each_date.replace('_', ' '), '9', 'teal', '8', 'Rate|||100000', '1', 'COVID19Cases'],
                               [each_date+'_deaths', 'Cumulative Number of Deaths on ' + each_date.replace('_', ' '), '9', 'teal', '8', 'Rate|||100000', '1', 'COVID19Deaths'],
                               [each_date + '_daily_change_confirmed_cases', 'Daily Change Confirmed Cases', '9', 'teal', '8', 'Rate|||100000', '1', 'COVID19Daily_Change_Confirmed_Cases'],
                               [each_date + '_growth_rate_confirmed_cases','Growth Rate of Cumulative Cases from Previous Day', '10', 'teal', '8', 'Rate|||100000', '0', 'COVID19Growth_Rate_Confirmed_Cases'],
                               [each_date + '_growth_rate_deaths', 'Growth Rate of Cumulative Deaths from Previous Day', '10', 'teal', '8', 'Rate|||100000', '0', 'COVID19Growth_Rate_Deaths'],
                               [each_date + '_weekly_change_confirmed_cases', 'Change in Cumulative Confirmed Cases from Previous Week', '9', 'teal', '8', 'Rate|||100000', '1', 'COVID19Weekly_Change_Confirmed_Cases'],
                               [each_date + '_weekly_cumulative_change_in_confirmed_cases_percent', 'Percent Change in Cumulative Confirmed Cases from Previous Week', '2', 'cold n hot', '8', 'Rate|||100000', '1', 'COVID19Weekly_Cumulative_Change_In_Confirmed_Cases_Percent'],
                               [each_date + '_monthly_change_confirmed_cases', 'Change in Cumulative Confirmed Cases from Previous Month', '9', 'teal', '8', 'Rate|||100000', '1', 'COVID19Monthly_Change_Confirmed_Cases'],
                               [each_date + '_monthly_cumulative_change_in_confirmed_cases_percent', 'Percent Change in Cumulative Confirmed Cases from Previous Month', '2', 'cold n hot', '8', 'Rate|||100000', '1', 'COVID19Monthly_Cumulative_Change_In_Confirmed_Cases_Percent'],
                               [each_date + '_weekly_change_deaths', 'Change in Cumulative Deaths from Previous Week', '9', 'teal', '8', 'Rate|||100000', '2', 'COVID19Weekly_Change_Deaths'],
                               [each_date + '_weekly_cumulative_change_in_deaths_percent', 'Percent Change in Cumulative Deaths from Previous Week', '2', 'cold n hot', '8', 'Rate|||100000', '1', 'COVID19Weekly_Cumulative_Change_In_Deaths_Percent'],
                               [each_date + '_monthly_change_deaths', 'Change in Cumulative Deaths from Previous Month', '9', 'teal', '8', 'Rate|||100000', '2', 'COVID19Monthly_Change_Deaths'],
                               [each_date + '_monthly_cumulative_change_in_deaths_percent', 'Percent Change in Cumulative Deaths from Previous Month', '2', 'cold n hot', '8', 'Rate|||100000', '1', 'COVID19Monthly_Cumulative_Change_In_Deaths_Percent'],
                               [each_date + '_percent_pop_confirmed_cases', 'Cumulative Percent of Population with Confirmed Cases', '2', 'cold n hot', '8', 'Rate|||100000', '1', 'COVID19Percent_Pop_Confirmed_Cases'],
                               [each_date + '_percent_pop_deaths', 'Cumulative Percent of Population Deaths', '9', 'teal', '8', 'Rate|||100000', '1', 'COVID19Percent_Pop_Deaths']
                               ]



        # variable_value_desc = [
        #     [each_date + '_cases', 'Cumulative Number of Confirmed Cases on ' + each_date.replace('_', ' '), '9', 'teal',
        #      '8', 'Rate|||100000', '1', 'COVID19Cases'],
        #     [each_date + '_deaths', 'Cumulative Number of Deaths on ' + each_date.replace('_', ' '), '9', 'teal', '1',
        #      'Add', '1', 'COVID19Deaths'],
        #     [each_date + '_daily_change_cases', 'Change in Cumulative Cases from Previous Day', '9', 'teal', '8', 'Rate|||100000',
        #      '1', ''],
        #     [each_date + '_daily_change_confirmed_cases', 'Daily Change Confirmed Cases', '9', 'teal', '8', 'Rate|||100000', '1', ''],
        #     [each_date + '_weekly_change_confirmed_cases', 'Change in Cumulative Confirmed Cases from Previous Week', '9',
        #      'teal', '8', 'Rate|||100000', '1', ''],
        #     [each_date + '_monthly_change_confirmed_cases', 'Change in Cumulative Confirmed Cases from Previous Month', '9',
        #      'teal', '1',
        #      'Add', '1', ''],
        #     [each_date + '_daily_change_deaths', 'Change in Cumulative Number of Deaths from Previous Day', '9', 'teal',
        #      '8', 'Rate|||100000', '1', ''],
        #     [each_date + '_weekly_change_deaths', 'Change in Cumulative Deaths from Previous Week', '9', 'teal', '8', 'Rate|||100000',
        #      '2', ''],
        #     [each_date + '_monthly_change_deaths', 'Change in Cumulative Deaths from Previous Month', '9', 'teal', '1',
        #      'Add', '2', ''],
        #     [each_date + '_daily_change_confirmed_cases_percent', 'Percent of Daily Change Confirmed Cases', '2',
        #      'cold n hot', '8', 'Rate|||100000', '1', 'COVID19Daily_Change_Confirmed_Cases_Percent'],
        #     [each_date + '_daily_change_deaths_percent', 'Percent of Daily Change Deaths', '2', 'cold n hot', '8', 'Rate|||100000',
        #      '1', ''],
        #     [each_date + '_daily_change_confirmed_cases_doubling_time',
        #      'Daily Change in Confirmed Cases By Doubling Time', '11', 'teal', '8', 'Rate|||100000', '1', ''],
        #     [each_date + '_daily_change_deaths_doubling_time', 'Daily Change in Deaths By Doubling Time',
        #      '11', 'teal', '8', 'Rate|||100000', '1', ''],
        #     [each_date + '_growth_rate_confirmed_cases', 'Growth Rate of Cumulative Cases from Previous Day', '10', 'teal',
        #      '8', 'Rate|||100000', '0', 'COVID19Growth_Rate_Confirmed_Cases'],
        #     [each_date + '_daily_change_in_growth_confirmed_cases_percent',
        #      'Percent Growth in Daily Change of Confirmed Cases from Previous Day', '2', 'cold n hot', '8', 'Rate|||100000', '0',
        #      'COVID19Daily_Change_In_Growth_Confirmed_Cases_Percent'],
        #     [each_date + '_percent_pop_confirmed_cases', 'Cumulative Percent of Population with Confirmed Cases', '2',
        #      'cold n hot', '8', 'Rate|||100000', '1', 'COVID19Percent_Pop_Confirmed_Cases'],
        #     [each_date + '_growth_rate_deaths', 'Growth Rate of Cumulative Deaths from Previous Day', '10', 'teal', '8',
        #      'Rate|||100000', '0', 'COVID19Growth_Rate_Deaths'],
        #     [each_date + '_percent_pop_deaths', 'Cumulative Percent of Population Deaths', '9', 'teal', '1',
        #      'Add', '1', 'COVID19Percent_Pop_Deaths'],
        #     [each_date + '_daily_change_in_growth_deaths_percent',
        #      'Percent Growth in Daily Change of Deaths from Previous Day', '2', 'cold n hot', '8', 'Rate|||100000', '1',
        #      'COVID19Daily_Change_In_Growth_Deaths_Percent'],
        #     [each_date + '_confirmed_rate_per_100k', 'Cumulative Confirmed Cases Rate per 100,000', '10', 'teal', '8',
        #      'Rate|||100000', '1', 'COVID19Confirmed_Rate_Per_100k'],
        #     [each_date + '_deaths_rate_per_100k', 'Cumulative Death Rate per 100,000', '10', 'teal', '8', 'Rate|||100000',
        #      '1', 'COVID19Deaths_Rate_Per_100k']
        #     ]
        #
        all_vars_list.extend(variable_value_desc)

    var_desc = pd.DataFrame(all_vars_list, columns=['code', 'description', 'formatting', 'palette', 'aggMethod', 'AggregationStr', 'bubbles', 'filter_rule'])

    var_desc.to_csv(str(config['config'])+'\\variable_descriptions.txt',
                    header=True, index=False, mode='w', encoding='utf-8')


def preprocess_output(config_file_path):
    columns_covid_cases = ['cases', 'deaths']
    config = load_config_file(config_file_path)
    st_raw = Path(config['state_github_raw_data'])
    cty_raw = Path(config['county_github_raw_data'])
    state_covid, cty_covid = read_data(st_raw, cty_raw)

    cty_geo_fixed = get_cty_geo_columns(cty_covid, config)
    state_geo_fixed = get_state_geo_columns(state_covid)

    appended_st_cty = append_st_cty(cty_geo_fixed, state_geo_fixed)

    us_cases = add_us_cases(appended_st_cty)

    all_data_appended = append_us_to_st_cty(us_cases, appended_st_cty)

    # write_name_qname_fips(all_data_appended)
    pivot_and_write_table(all_data_appended, columns_covid_cases, config)

    write_geo_file(config['geo_acs18_5yr_raw_data'], all_data_appended, config)

    files_list(config)

    variable_description(all_data_appended, config)


if __name__ == '__main__':
    config = argv[1]
    preprocess_output(config)
