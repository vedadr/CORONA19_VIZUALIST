"""
This script will take .csv data from provided folder in Config file.
1. It will create _FIPS, _NAME, QNAME columns
2. Create empty rows for missing geographies
3. Split tables if they have more than specified numbers of columns
    (define column numbers in config file!)
4. Create table_names table in db with proper names

version 5.1

"""
import datetime
import multiprocessing as mp
import os
import sys
import threading
import time
from pathlib import Path
from sys import argv

import numpy as np
import pandas as pd
import sqlalchemy as sqla
import yaml
from loguru import logger
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.types import CHAR
from sqlalchemy.types import FLOAT
from sqlalchemy.types import INTEGER
from sqlalchemy.types import TEXT
from sqlalchemy.types import VARCHAR
from sqlalchemy.exc import ProgrammingError

try:
    from sedatatools.sedatatools.tools.connectors import prime
except ImportError:
    from sedatatools.tools.connectors import prime

pd.options.mode.chained_assignment = None
start_time = time.time()


def get_sl_from_table_id(sl):
    return sl[:sl.index('_')]


def create_db(config):
    engine = create_engine_for_db({
        'databaseName': 'master',
        'trustedConnection': config['trustedConnection'],
        'serverName': config['serverName'],
        'sqlServerDriverName': config['sqlServerDriverName'],
    })
    connection = engine.connect()

    if config['serverName'] == 'prime':
        data_file_folder = "X:\MSSQL_Data"
    else:
        data_file_folder = "C:\Program Files\Microsoft SQL Server\MSSQL12.SQLEXPRESS\MSSQL\DATA"

    try:
        logger.info(
            f'Trying to create database {config["databaseName"]} on {config["serverName"]} ...',
        )
        query = f'CREATE DATABASE {config["databaseName"]} ON ( NAME = {config["databaseName"]}_dat, ' \
                f'FILENAME="{data_file_folder}\{config["databaseName"]}.mdf") ' \
                f'LOG ON ( NAME = {config["databaseName"]}_log, ' \
                f'FILENAME="{data_file_folder}\{config["databaseName"]}_log.mdf");'

        connection.execute(query)

    except ConnectionError as ce:
        logger.warning(ce)

    except sqla.exc.ProgrammingError as exc:
        logger.warning(f'Cannot create a database: {exc}')


def create_engine_for_db(config):
    if config['trustedConnection']:
        engine = create_engine(
            f'mssql+pyodbc://{config["serverName"]}/{config["databaseName"]}?driver={config["sqlServerDriverName"]}',
            connect_args={'autocommit': True},
        )
    else:
        prime_conf = prime.Prime()
        user_name = prime_conf.user_name
        password = prime_conf.password

        engine = create_engine(
            f'mssql+pyodbc://{user_name}:{password}@PRIME/{config["databaseName"]}?driver={config["sqlServerDriverName"]}',
            connect_args={'autocommit': True},
        )

    return engine


def load_config_file(config_file_path):
    with open(config_file_path, 'r') as f:
        try:
            config = yaml.load(f, Loader=yaml.FullLoader)
        except yaml.YAMLError as ex:
            logger.warning(ex)

    config['metadataOutputPath'] = Path(config['metadataOutputPath'])
    config['variableDefinitionsPath'] = Path(config['variableDefinitionsPath'])
    config['tableDefinitionsPath'] = Path(config['tableDefinitionsPath'])
    config['sourceDirectory'] = Path(config['sourceDirectory'])
    config['configDirectory'] = Path(config['configDirectory'])

    return config


def verify_config_file(config_file_path):
    errors = []
    warnings = []

    if Path.is_file(config_file_path):
        logger.warning(
            "Error: Config file doesn't exist on selected location: " + config_file_path + " !",
        )
        return False

    config = load_config_file(config_file_path)
    if len(config['connectionString']) == 0 or 'connectionString' not in config.keys():
        errors.append(
            'Error: Connection string not set properly in config file!',
        )

    if len(config['projectName']) == 0 or 'projectName' not in config.keys():
        errors.append('Error: Project name not set properly in config file!')
    if len(config['projectId']) == 0 or 'projectId' not in config.keys():
        errors.append('Error: Project id not set properly in config file!')
    if type(config['projectDate']) is not datetime.date or 'projectDate' not in config.keys():
        errors.append('Error: Project date not set properly in config file!')
    if len(config['databaseName']) == 0 or 'databaseName' not in config.keys():
        errors.append('Error: Database name not set properly in config file!')
    if len(str(config['projectYear'])) != 4 or 'projectYear' not in config.keys():
        errors.append('Error: Project year not set properly in config file!')
    if config['projectYear'] > datetime.datetime.now().year:
        warnings.append('Warning: Project year is set in future.')
    if not (Path.is_file(config['variableDescriptionLocation']) or
            Path.is_dir(config['variableDescriptionLocation'])):
        errors.append('Error: Something is wrong with variable info location!')

    for warn in warnings:
        logger.warning(warn)

    if len(errors) == 0:
        return True

    else:
        for err in errors:
            logger.warning(err)
        return False


def get_file_names_list(config):
    """
    This function will check if there is files list (fl.txt) file with defined order for table input.
    If there is no defined order, read data from source file by regular order
    :param config: Configuration file
    :return:
    """

    if not Path.is_dir(config['sourceDirectory']):
        logger.warning(
            "Error: Source file doesn't exist on selected location: " +
            config['sourceDirectory'] + " !",
        )
        return False

    if not Path.is_file(config['tableDefinitionsPath']):
        logger.warning(
            'There is no files list defined, reading files from source directory in random order.',
        )

        items = os.listdir(config['sourceDirectory'])

        files_list = [Path.joinpath(config['sourceDirectory'], file_name)
                      for file_name in items if file_name.endswith('csv')]

        if not files_list:
            logger.warning('Folder is empty!')
        else:
            return files_list

    else:
        table_definitions = pd.read_csv(config['tableDefinitionsPath'])[
            'file_name'
        ].apply(lambda x: Path.joinpath(config['sourceDirectory'], x))

        if table_definitions.__len__() == 0:
            logger.warning('Folder is empty!')
        else:
            return table_definitions


def capitalize_level_names(geo_level):
    """
    This function returns list of geo names capitalized (NATION, COUNTY...)
    :param geo_level:
    :return: List of geo level names capitalized
    """
    geo_names_cap = []
    chars_for_removal = [
        '(', ')', '&', '/', ',', '.',
        '\\', '\'', '&', '%', '#',
    ]

    for gl in geo_level:
        for ch in chars_for_removal:
            if ch in gl:
                gl = gl.replace(ch, ' ')
        gl = gl.strip()
        while '  ' in gl:
            gl = gl.replace('  ', '')
        if ' ' in gl:
            blank_pos = [i for i, letter in enumerate(gl) if letter == ' ']
            geo_names_cap.append(
                gl[0].upper() + ''.join([
                    gl[i + 1]
                    for i in blank_pos
                ]).upper(),
            )
        else:
            geo_names_cap.append(gl.upper())

    return geo_names_cap


def create_system_columns_fips(table_id, table, config, level_names):
    """
    :param table_id: It is a key from Table holder dictionary... it contains information about sl levels
    :param table: Table contains values from Table holder dictionary.. if key is SL010(sl_level) then table contains
    all data about that level (all geo info and table content to be writen into a db)
    :param config: configuration file
    :param level_names: level names to be capitalized (NATION, STATE...)
    :return: Table that contains all system variables (SL010_FIPS, SL010_NAME, NATION, STATE...)
    """

    geo_level_info = config['geoLevelInformation']

    names = capitalize_level_names(level_names)

    sl_level = get_sl_from_table_id(table_id)

    primary_keys = get_primary_keys(sl_level, geo_level_info, for_fips=True)

    cut_cum = 0
    for primary_key in primary_keys:
        current_geo_level_pos = [
            index for index, geo_level in enumerate(geo_level_info) if geo_level[0] == primary_key
        ][0]
        if primary_key == "SL010":
            table[f"{primary_key}_FIPS"] = '00'
            table[f"{primary_key}_NAME"] = 'United States'
            table[f"{primary_key}_FULL_FIPS"] = '00'
            if names[current_geo_level_pos] == capitalize_level_names(geo_level_info[current_geo_level_pos])[1]:
                table[names[current_geo_level_pos]] = '00'
        elif cut_cum == 0:
            table[f"{primary_key}_FIPS"] = table['GeoID'].str[-int(geo_level_info[current_geo_level_pos][3]):]
            table[f"{primary_key}_NAME"] = table['GeoID'].str[-int(geo_level_info[current_geo_level_pos][3]):]
            table[names[current_geo_level_pos]] = table['GeoID'].str[-int(geo_level_info[current_geo_level_pos][3]):]
            table[f"{primary_key}_FULL_FIPS"] = table['GeoID'].str[-int(geo_level_info[current_geo_level_pos][2]):]
            cut_cum = int(geo_level_info[current_geo_level_pos][3])
        else:
            table[f"{primary_key}_FIPS"] = table['GeoID'].str[-int(geo_level_info[current_geo_level_pos][3])-cut_cum:-cut_cum]
            table[f"{primary_key}_NAME"] = table['GeoID'].str[-int(geo_level_info[current_geo_level_pos][3])-cut_cum:-cut_cum]
            table[names[current_geo_level_pos]] = table['GeoID'].str[-int(geo_level_info[current_geo_level_pos][3])-cut_cum:-cut_cum]
            table[f"{primary_key}_FULL_FIPS"] = table['GeoID'].str[:-cut_cum]
            cut_cum += int(geo_level_info[current_geo_level_pos][3])

    return table


def create_system_columns_names(table, all_geographies, sl_levels):
    """
    Add geo names for sum levels and crate QNames ... SL010_NAME, SL040_NAME.....
    :param table:
    :param all_geographies:
    :param sl_levels: list of dictionary keys
    :return:
    """

    sl_levels = [get_sl_from_table_id(sl) for sl in sl_levels]
    for level in sl_levels:
        try:
            table = pd.merge(
                table, all_geographies[all_geographies['SUMLEV'] == level].drop(
                    ['TYPE', 'SUMLEV'], axis=1,
                ),
                how='left',
                left_on=[level + '_FULL_FIPS'],
                right_on=['GeoID'],
            )
            table.drop(level + '_NAME', axis=1, inplace=True)
            table.rename(
                index=str, columns={
                    'NAME_y': level + '_NAME', 'NAME_x': 'NAME', 'GeoID_x': 'GeoID',
                },
                inplace=True,
            )
        except KeyError:
            continue

    cols_to_join = [
        tn for tn in table.columns if '_NAME' in tn and tn != 'SL010_NAME'
    ]

    for col in reversed(cols_to_join):
        if table[col].isnull().any():
            continue
        else:
            table['QName'] = table['QName'] + ', ' + table[col]

    table['QName'] = table['QName'].str[2:]
    return table


def write_table_names(table_info, config, engine):
    connection = engine.connect()

    table_names_content = []

    for name in table_info:
        code_name = [n[6:] for n in list(name[1])]

        table_names_content.extend(
            [[os.path.basename(name[0]), cn, config['projectYear']] for cn in set(code_name)],
        )

    table_names = pd.DataFrame(
        table_names_content, columns=['descName', 'code_Name', 'projectYear'],
    )

    # checks if table_names exist and appends new values for respective project year
    if engine.dialect.has_table(engine, 'table_names'):
        meta_data = MetaData(bind=engine, reflect=True)
        table = meta_data.tables['table_names']

        select_st = sqla.select([table])
        result = connection.execute(select_st)
        rows = result.fetchall()
        if len(rows) != 0:
            try:
                connection.execute(table.insert(), table_names.to_dict('records'))
            except ProgrammingError as e:
                logger.error(f'Error occurred while writing table_names! More details: {e}')
    else:
        table_names.to_sql('table_names', connection, index=False)


def create_table_id(sumlev, suffix_width, sub_counter, table_counter):
    """
    This function is used while splitting table if table is larger than defined, and returns suffix of tables as str.
    SL010_001...SL010_001001
    :param sumlev: Summary level
    :param suffix_width: defined suffix width, in this example 3 characters
    :param sub_counter: counter if table was splitted into smaller tables
    :param table_counter: counter if table is not splitted
    :return:
    """

    if sub_counter == 0:
        additional_zeros = ''.join(
            ['0' for _ in range(suffix_width - len(str(table_counter)))],
        )
        return sumlev + '_' + additional_zeros + str(table_counter)
    else:
        additional_zeros_sub = ''.join(
            ['0' for _ in range(suffix_width - len(str(sub_counter)))],
        )
        additional_zeros = ''.join(
            ['0' for _ in range(suffix_width - len(str(table_counter)))],
        )
        return sumlev + '_' + additional_zeros + str(table_counter) + additional_zeros_sub + str(sub_counter)


def check_last_table_name_if_exists(engine, config):
    """
    This will find the last table number in db
    :param engine:
    :param config:
    :return:
    """

    db_table_names = engine.table_names()

    unique_tables = []

    for table in db_table_names:

        table_suffix = table.split("_")[-1]

        if table_suffix != "geo" and table != "table_names" and '_Names_' not in table:
            unique_tables.append(table_suffix)

    if len(unique_tables) > 0:

        last_table_suffix = int(sorted(set(unique_tables))[-1]) + 1

    else:
        last_table_suffix = config['tableNumberingStartsFrom']

    return last_table_suffix


def merge_geo_data_tables(file_path, config_files):
    """
    The function processes only one file at a time..
    :param file_path: File path to file
    :param config_files: Config file
    :return:
    """

    # Load data
    # Read original tables and all geo types
    # Merge all geo types and tables

    original_table = pd.read_csv(file_path, dtype={'GeoID': 'str'})
    logger.info('Writing table - ' + os.path.basename(file_path))

    try:
        all_geo_types_and_sumlev = pd.read_csv(
            Path.joinpath(
                config_files['configDirectory'], 'all_geographies.csv',
            ), dtype={'GeoID': 'str'},
        )
    except UnicodeDecodeError:
        all_geo_types_and_sumlev = pd.read_csv(
            Path.joinpath(
                config_files['configDirectory'], 'all_geographies.csv',
            ), dtype={'GeoID': 'str'},
            encoding="ISO-8859-1",
        )

    if "SUMLEV" in original_table.columns:
        geotypes_org_tables_merged = pd.merge(
            all_geo_types_and_sumlev, original_table, how='left', on=['SUMLEV', 'GeoID'],
        )
    else:
        geotypes_org_tables_merged = pd.merge(
            all_geo_types_and_sumlev, original_table, how='left', on='GeoID',
        )

    # TODO: add test to check if merge is done properly

    return all_geo_types_and_sumlev, geotypes_org_tables_merged


def create_table(file_path, config_files, file_counter, engine):
    # Select list of geo levels from config file
    geo_level_info = config_files['geoLevelInformation']

    all_geographies, geo_types_org_tables_merged = merge_geo_data_tables(
        file_path, config_files,
    )

    # Create list of summary levels
    sumlev_ids = [k[0] for k in geo_level_info]

    # Place holder for system column names, this will be populated later
    for k in sumlev_ids:
        geo_types_org_tables_merged[k + "_FIPS"] = None
        geo_types_org_tables_merged[k + "_NAME"] = None
        geo_types_org_tables_merged['QName'] = ''

    # Capitalize sum level names
    geo_level_names = [k[1] for k in geo_level_info]
    geo_names_cap = capitalize_level_names(geo_level_names)
    for name in geo_names_cap:
        geo_types_org_tables_merged[name] = None

    # Check if table size is larger than defined number of columns, and split it if required
    table_holder = {}
    table_counter = file_counter

    # ToDo: For table size larger than defined, split columns proprerly -
    #  at this time this function doesn't split tables properly...
    #  check function again and speed it up!

    for sumlev in sumlev_ids:
        loaded_table = geo_types_org_tables_merged[geo_types_org_tables_merged['SUMLEV'] == sumlev]

        system_columns = ['GeoID', 'Geo', 'SUMLEV', 'TYPE', 'NAME', 'QName'] + [
            sl +
            '_FIPS' for sl in sumlev_ids
        ] + [sl + '_NAME' for sl in sumlev_ids] + geo_names_cap
        column_prefix = config_files['projectId'] + '_' + str(table_counter).zfill(
            3,
        ) + '_' if config_files['projectId'] != '' else ''

        data_columns = [
            column_prefix + c for c in loaded_table.columns if c not in system_columns
        ]

        if len(data_columns) > config_files['maxTableWidth']:
            splitted_columns = []
            sub_table_counter = 1

            for ind, column in enumerate(data_columns, 1):

                if (len(data_columns) - ind) < config_files['maxTableWidth']:
                    if (len(splitted_columns) < config_files['maxTableWidth']) & (len(data_columns) - ind == 0):
                        splitted_columns.append(column)
                        table_holder[create_table_id(
                            sumlev, 3, sub_table_counter, table_counter,
                        )] = loaded_table[system_columns + splitted_columns]
                        break

                    elif len(splitted_columns) < config_files['maxTableWidth']:
                        splitted_columns.append(column)

                    elif len(splitted_columns) == config_files['maxTableWidth']:
                        table_holder[create_table_id(
                            sumlev, 3, sub_table_counter, table_counter,
                        )] = loaded_table[system_columns + splitted_columns]
                        splitted_columns = []
                        sub_table_counter += 1
                        splitted_columns.append(column)

                    else:
                        table_holder[create_table_id(
                            sumlev, 3, sub_table_counter, table_counter,
                        )] = loaded_table[system_columns + splitted_columns]
                        splitted_columns = []
                        sub_table_counter += 1

                elif ind % config_files['maxTableWidth'] != 0:
                    splitted_columns.append(column)
                else:
                    splitted_columns.append(column)
                    table_holder[create_table_id(
                        sumlev, 3, sub_table_counter, table_counter,
                    )] = loaded_table[system_columns + splitted_columns]
                    splitted_columns = []
                    sub_table_counter += 1
        else:
            table_holder[create_table_id(
                sumlev, 3, 0, table_counter,
            )] = loaded_table

    # for each sum level create table with system columns - _FIPS & _NAMES
    connection = engine.connect()

    prefix_to_table = config_files['projectId'] + \
                      '_' if config_files['projectId'] != '' else ''

    processes = []  # list that contains all processes used for database writing

    for table_id, table in table_holder.items():
        try:
            table.columns = [
                column_prefix + c if c not in system_columns else c for c in table.columns
            ]

            table_holder[table_id] = create_system_columns_fips(
                table_id, table, config_files, geo_level_names,
            )

            table_holder[table_id] = create_system_columns_names(
                table, all_geographies, table_holder.keys(),
            )

            table_holder[table_id].drop(
                list(table_holder[table_id].filter(regex='_FULL_FIPS')), axis=1, inplace=True,
            )

            table_holder[table_id].drop(['GeoID_y'], axis=1, inplace=True)

            if (table_holder[table_id]['QName'] == '').all():
                table_holder[table_id]['QName'] = 'United States'

            table_holder[table_id].rename(
                columns={'GeoID': 'FIPS'}, inplace=True,
            )
            #  TODO parametrize max number of processes and error handling
            s = mp.Process(
                target=to_sql_using_threads,
                args=(
                    table_holder[table_id],
                    prefix_to_table + table_id, config_files,
                ),
            )
            s.start()
            processes.append(s)

        except ValueError:  # TODO make it more specific

            connection.execute("DROP TABLE " + prefix_to_table + table_id)
            s = mp.Process(
                target=to_sql_using_threads, args=(
                    table_holder[table_id], prefix_to_table +
                    table_id, config_files,
                ),
            )
            s.start()
            processes.append(s)

    for process in processes:
        process.join()

    logger.info("--- %s seconds ---" % (time.time() - start_time), )
    return [file_path, table_holder.keys()]


def create_sql_columns(dataframe, table_name, config):
    """
    The function iterates over columns of the dataframe and
    creates Sqlalchmemy Column objects with appropriate
    attributes and constraints (i.e. NOT NULL or PRIMARY KEY)
    :param dataframe: Dataframe as Pandas Dataframe object TODO find a better name
    :param table_name: Name of the table
    :param config: Config file
    :return: list of Sqlalchemy Column objects
    """
    geo_level_info = config['geoLevelInformation']
    list_of_sql_columns = []
    primary_keys = get_primary_keys(table_name[-9:-4], geo_level_info)
    names = capitalize_level_names([
        geo_level[1]
        for geo_level in geo_level_info
    ])
    fips_length = 2 if table_name[-9:-4] == 'SL010' else [i for i in geo_level_info if i[0]
                                                          == table_name[-9:-4]][0][2]  # gets the fips length value for the respective geo level

    for column in dataframe:  # TODO find a better name
        is_null = dataframe[column].isnull().any()  # to avoid multiple conditions in further part of the code
        col_cell = dataframe[column][-dataframe[column].isna()][0] if len(dataframe[column][-dataframe[column].isna()]) > 0 else float('nan')  # check if column has any non-null
        # values and assigns the first
        # non-null value to col_cell

        dataframe[column][-dataframe[column].isna()] = dataframe[column][-dataframe[column].isna()].astype(type(col_cell)) #make sure all non-null values to be of same type
        col_type = get_col_type(col_cell)
        if column == 'NAME' or column == 'QName':
            list_of_sql_columns.append(
                Column(name=column, type_=VARCHAR(length='max'), nullable=is_null),
            )
        elif column == 'FIPS':
            list_of_sql_columns.append(
                Column(name=column, type_=CHAR(length=fips_length), nullable=is_null),
            )
        elif "_FIPS" in column:
            current_geo_level = [
                geo_level for geo_level in geo_level_info if geo_level[0] == column[:5]
            ][0]
            # gets the partial fips length value for the current geo level in the for loop
            char_length = 2 if column[:5] == 'SL010' else current_geo_level[3]

            if column[:5] in primary_keys:
                list_of_sql_columns.append(Column(name=column, type_=CHAR(length=char_length), primary_key=True, nullable=is_null))
            else:
                list_of_sql_columns.append(Column(name=column, type_=VARCHAR(length='max') if is_null else CHAR(length=char_length), nullable=is_null))

        elif column in names:
            char_length = 2 if 'SL010' in geo_level_info[names.index(column)][0] else geo_level_info[names.index(
                column,
            )][3]  # gets the partial fips length value for the
            # current geo level in the for loop
            list_of_sql_columns.append(Column(
                name=column, type_=VARCHAR(length='max') if is_null else CHAR(
                    length=char_length,
                ), nullable=is_null,
            ))
        elif "NAME" in column:
            list_of_sql_columns.append(
                Column(name=column, type_=TEXT, nullable=True),
            )
        else:
            list_of_sql_columns.append(
                Column(name=column, type_=col_type, nullable=True),
            )

    return list_of_sql_columns


def get_primary_keys(sum_level, geo_level_info, for_fips = False):
    # this list should always return just one element, if not then sumlevs are not unique
    low_fips_pos = [
        i for i, k in enumerate(
            geo_level_info,
        ) if k[0] == sum_level
    ][0]
    full_fips_list = [sum_level]
    for i in reversed(range(low_fips_pos)):
        if(geo_level_info[i][0] == "SL010" and for_fips and sum_level != "SL010"):
            full_fips_list.append(geo_level_info[i][0])
            continue
        # stop adding sumlevs if you find geo level with indent 0 or fips length of 0
        if geo_level_info[i][4] == 0 or geo_level_info[i][2] == '0':
            break
        # if you find summary_level with same indent as previous or greater than wanted then skip it
        if geo_level_info[i][4] == geo_level_info[i + 1][4] or geo_level_info[i][4] >= geo_level_info[low_fips_pos][4]:
            continue

        full_fips_list.append(geo_level_info[i][0])

    return full_fips_list


def get_col_type(data_frame_cell):
    """
    The function checks the data type of a Pandas Dataframe column
    and returns the corresponding Sqlalchemy Type object
    :param data_frame_cell: first element of a Pandas Dataframe column
    :return: corresponding Sqlalchemy Type object
    """
    if isinstance(data_frame_cell, str) or data_frame_cell is None:
        return TEXT
    elif isinstance(data_frame_cell, float):
        return FLOAT
    else:
        return INTEGER


def create_sql_table(name, engine, *cols):
    """
    The function creates a Sqlalchemy Table object
    and writes it to the database
    :param name: name of the tabke
    :param engine: Salalchemy Engine object
    :param cols: Sqlalchemy Column objects for the table
    :return: Sqlalchemy Table object
    """
    meta = MetaData()
    meta.reflect(bind=engine)
    if name in meta.tables:
        return

    table = Table(name, meta, *cols)
    table.create(engine)
    return table


def to_sql_using_threads(table_for_db, table_name, config):
    """
    The function uses the data from the Pandas Dataframe
    as a list of dictionaries for each row and writes that
    data into the database
    :param table_for_db: name of the tabke  
    :param table_name: Salalchemy Engine object
    :param config: Sqlalchemy Column objects for the table
    :return:
    """

    # all file handles and database connection must be created inside the function
    engine = create_engine_for_db(config)
    conn = engine.connect()
    list_col = create_sql_columns(table_for_db, table_name, config)
    if engine.dialect.has_table(engine, table_name):
        meta_data = MetaData(bind=engine, reflect=True)
        table = meta_data.tables[table_name]
        table.drop(engine)
    sql_table = create_sql_table(table_name, engine, *list_col)
    table_for_db = table_for_db.astype(object).where(pd.notnull(table_for_db), None)
    table_for_db_dict = table_for_db.to_dict('records')

    chunk_size = 1000

    if len(table_for_db) > 1000:  # TODO get this from shape
        initial_chunk = 100
        if len(table_for_db) > chunk_size:
            # TODO check if we can use BULK insert for this
            try:
                que = conn.execute(sql_table.insert(), table_for_db_dict[:initial_chunk])
            except ProgrammingError as e:
                logger.error(f'Error occurred while writing {table_for_db}! More details: {e}')

            for i in range(int(np.ceil((len(table_for_db) - initial_chunk) / chunk_size))):
                if i == int(np.ceil((len(table_for_db) - initial_chunk) / chunk_size)):
                    table_for_db2_dict = table_for_db_dict[initial_chunk + (i + 1) * chunk_size:]
                else:
                    table_for_db2_dict = table_for_db_dict[
                               initial_chunk + i *
                               chunk_size:initial_chunk + (i + 1) * chunk_size
                               ]
                conn2 = engine.connect()
                try:
                    t = threading.Thread(
                        target=conn2.execute,  # to avoid deadlocks we must use a new connection
                        args=[sql_table.insert(), table_for_db2_dict],
                    )
                    t.start()
                except ProgrammingError as e:
                    logger.error(f'Error occurred while writing {table_name}! More details: {e}')

    else:
        try:
            conn.execute(sql_table.insert(), table_for_db_dict)
        except ProgrammingError as e:
            logger.error(f'Error occurred while writing {table_name}! More details: {e}')

    for p_key in sql_table.primary_key.columns.values():
        logger.info(f'Primary key: {p_key.name} created for: {table_name}')

    logger.info(f'{table_name} written to sql. with alchemy')
    logger.info("--- %s seconds ---" % (time.time() - start_time), )


def process_data(configuration):
    files_list = get_file_names_list(configuration)

    table_names_info = []

    engine = create_engine_for_db(configuration)

    file_counter = int(check_last_table_name_if_exists(engine, configuration))

    written_tables = pd.DataFrame()
    if engine.dialect.has_table(engine, 'table_names'):

        query = "SELECT descName FROM table_names"
        written_tables = pd.read_sql(query, engine)

    if len(written_tables) > 0:
        written_tables_to_db = written_tables.iloc[:, 0].apply(lambda x: Path.joinpath(configuration['sourceDirectory'], x))
        files_list = set(written_tables_to_db).symmetric_difference(files_list)

    for file_path in files_list:
        if not Path.is_file(file_path):
            logger.warning(f'File {file_path} does not exist, skipping...')
            continue
        table_names_info.append(create_table(
            file_path, configuration, file_counter, engine,
        ))
        file_counter += 1

    write_table_names(table_names_info, configuration, engine)


def process_and_load_data(config_file_path=argv[1]):
    config = load_config_file(config_file_path)

    create_db(config)

    process_data(config)


if __name__ == '__main__':

    process_and_load_data()

    end_time = time.time()
    hours, rem = divmod(end_time - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    logger.info("Finished in {:0>2}:{:0>2}:{:05.2f}".format(
        int(hours), int(minutes), seconds,
    ))
