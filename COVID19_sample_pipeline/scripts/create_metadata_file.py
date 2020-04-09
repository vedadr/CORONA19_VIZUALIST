"""
This script will generate metadata file for the newest pipeline

notes:
geos - geographical identifiers (e.g. FIPS for US)

Don't forget to bump the version number up every time you change something
v3.7.0
"""
import optparse
import sys
import uuid
from sys import argv

import lxml.etree as et
from loguru import logger
from lxml.builder import ElementMaker
from pymssql import connect
from scripts.helpers.commons import check_data_type
from scripts.helpers.commons import create_acronym
from scripts.helpers.commons import get_config
from scripts.helpers.commons import get_metadata_from_input_file

try:
    from sedatatools.sedatatools.tools.connectors.connectors import Connect
except ImportError:
    from sedatatools.tools.connectors.connectors import Connect


def get_all_guids(config):
    conf = get_config(config)
    try:
        tree = et.parse(conf['metadataOutputPath']+conf['metadataFileName'])
    except OSError:
        logger.warning("Looks like no metadata file has been created already, is this the first run? Exiting...")
        sys.exit()

    root = tree.getroot()

    all_guids = {}

    for elem in tree.iter():
        if 'GUID' in elem.attrib:
            try:
                all_guids[elem.attrib['name']] = elem.attrib['GUID']
            except KeyError:
                try:
                    all_guids[elem.attrib['Name']] = elem.attrib['GUID']
                except KeyError:
                    all_guids[elem.attrib['GeoTypeName']] = elem.attrib['GUID']
        if elem.tag == 'geoTypes':
            for child in elem:
                all_guids[child.attrib['Label']] = child.attrib['GUID']
    return all_guids


def get_table_geo_columns(table_fips, geo_level_info):
    """
    This will create a string with list of geo columns (SL050_FIPS, SL040_FIPS, etc.) based on nesting information
    (DB Conn. Info) to be used as primary keys.
    :param table_fips: Fips code for the geo that table is on e.g. for county level table it's FIPS_SL050
    :param geo_level_info: GeoInfo from config file
    :return: String with list of FIPS columns
    """
    # this list should always return just one element, if not then sumlevs are not unique
    low_fips_pos = [
        i for i, k in enumerate(
            geo_level_info,
        ) if k[0] == table_fips.replace('_FIPS', '')
    ][0]
    full_fips_list = [table_fips]
    for i in reversed(range(low_fips_pos)):
        # stop adding sumlevs if you find geo level with indent 0 or fips length of 0
        if geo_level_info[i][4] == 0 or geo_level_info[i][2] == '0':
            break
        # if you find summary_level with same indent as previous or greater than wanted then skip it
        if geo_level_info[i][4] == geo_level_info[i + 1][4] or geo_level_info[i][4] >= geo_level_info[low_fips_pos][4]:
            continue

        full_fips_list.append(geo_level_info[i][0] + '_FIPS')

    full_fips_list = ','.join(list(reversed(full_fips_list)))
    return full_fips_list


def get_table_metadata_from_db(config):
    """
    Get table descriptions from table_names table in database, make it prettier and return it as a dictionary

    :type config: configuration file
    :return: Dictionary of metadata tables
    """
    # extensions to remove from input file e.g. Sex_by_Age.csv > Sex_by_Age
    extension = ['.txt', '.csv', '.tsv']
    meta_table_dictionary = {}

    conn = config['connection']
    cursor = conn.cursor()
    # TODO change this ASAP

    cursor.execute(
        f"SELECT * FROM table_names where CONVERT(VARCHAR, projectYear) = '{str(config['projectYear'])}'",
    )

    table_names = cursor.fetchall()

    # exit if table_names table empty
    if len(table_names) == 0:
        logger.info("Error: Table names does not exist in database!")
        sys.exit()

    meta_data = []
    for ext in extension:
        for el in table_names:
            if ext in el[0]:
                # remove extension and data set ID (everything until first '_')
                # NEW if added to skip datasets without dataset id, this will be the approach in the new processing pipeline

                if '_' not in el[0]:
                    meta_data.append([el[0].replace(ext, ''), el[1]])
                else:
                    meta_data.append(
                        [el[0].replace(ext, ''), el[1]],
                    )

    for i in meta_data:
        meta_table_dictionary[i[1]] = i[0]

    return meta_table_dictionary


def get_tables_from_db(config):
    """
    Get list of tables from db, make it unique on metadata level and return it as a dictionary
    :param config:
    :return: list of tables in database
    """
    conn = config['connection']
    cursor = conn.cursor()

    # TODO fix this ASAP, it shouldn't querry the db this way
    cursor.execute(
        f"SELECT name FROM sys.objects "
        f"WHERE type_desc = 'USER_TABLE' "
        f"and name <> 'table_names' "
        f"and left(name,1) <> '_' "
        f"and name <> 'sysdiagrams' "
        f"order by modify_date",
    )

    table_list = [
        str(*db_table_name)
        for db_table_name in cursor.fetchall() if db_table_name != 'table_names'
    ]
    dict_tables_and_vars = {}
    cursor = conn.cursor()
    for table_name in table_list:
        sql = "select DATA_TYPE from information_schema.columns where TABLE_NAME = '" + table_name + "'"
        cursor.execute(sql)
        list_of_types = cursor.fetchall()

        var_types = [check_data_type(datum[0]) for datum in list_of_types]

        # TODO check if this will work with TOP 1
        sql_tab = 'SELECT TOP 1 * FROM ' + table_name
        cursor.execute(sql_tab)

        dict_tables_and_vars[table_name] = [
            var_desc[0]
            for var_desc in cursor.description
        ]
        attrib_type = [var_desc[1] for var_desc in cursor.description]
        dict_tables_and_vars[table_name] = list(
            zip(dict_tables_and_vars[table_name], var_types, attrib_type),
        )

    dict_tables_and_vars_unique = {}
    for table_name, table_variables in dict_tables_and_vars.items():
        values = []
        pos_t = [
            index for index, char in enumerate(
                table_name,
            ) if char == '_'
        ][1] + 1
        for variable in table_variables:
            # skip system required names
            if len([index for index, char in enumerate(variable[0]) if char == '_']) < 3:
                continue
            pos_v = [
                index for index, char in enumerate(
                    variable[0],
                ) if char == '_'
            ][2] + 1

            values.append([variable[0][pos_v:], variable[1]])
        dict_tables_and_vars_unique[table_name[pos_t:]] = values

    #dict_tables_and_vars = dict(sorted(list(dict_tables_and_vars.items())))
    dict_tables_and_vars = dict(sorted(dict_tables_and_vars.items(), reverse=True))
    return dict_tables_and_vars


def get_geo_type(geo_level_info, all_guids):
    """
    Get list of geo_types in project
    :param geo_level_info: List from config file
    :return:
    """

    e = ElementMaker()

    summary_level_information = geo_level_info
    plural_forms = {'County': 'Counties', 'State': 'States'}
    for sum_level in summary_level_information:
        if sum_level[1] not in summary_level_information:
            plural_forms[sum_level[1]] = sum_level[1]

    # create names of relevant geos
    relevant_geos = ','.join([
        create_acronym(i[1])
        for i in summary_level_information
    ])

    result = []

    for summary_level in summary_level_information:
        partial_fips_field = create_acronym(summary_level[1])
        result.append(
            e.geoType(
                e.Visible('true'),
                GUID=all_guids.get(summary_level[1], str(uuid.uuid4())),
                Name=summary_level[0],
                Label=summary_level[1],
                QLabel=summary_level[1],
                RelevantGeoIDs='FIPS,NAME,QName,' + relevant_geos,
                PluralName=plural_forms[summary_level[1]],
                fullCoverage='true',
                majorGeo='true',
                # GeoAbrev = summary_level[0],#'us, nation', COMMENTED BECAUSE IN ACS 2011 EXAMPLE IT WAS
                # MISSING!?
                Indent=str(int(summary_level[4])),
                Sumlev=summary_level[0].replace('SL', ''),
                FipsCodeLength=summary_level[2],
                FipsCodeFieldName='FIPS',
                FipsCodePartialFieldName=partial_fips_field,
                FipsCodePartialLength=str(summary_level[3]),
            ),
        )
    return result


def get_geo_survey_datasets(config, all_guids):
    """
    Get datasets information for GeoSurveyDataset, (Db Conn. Info in metadata editor)
    :param config: configuration dictionary
    :return: List of data sets
    """
    e = ElementMaker()
    result = []

    for summary_level in config['geoLevelInformation']:
        geo_id_suffix = get_geo_id_db_table_name(summary_level[0], config)

        result.append(e.dataset(
            GUID=all_guids.get(config['projectId'] + '_' + summary_level[0] + '_', str(uuid.uuid4())),
            GeoTypeName=summary_level[0],  # SL040
            DbConnString=config['metadataConnectionString'],
            DbName=config['databaseName'],
            GeoIdDbTableName=config['projectId'] + '_' + summary_level[0] + geo_id_suffix,
            IsCached='false',
            DbTableNamePrefix=config['projectId'] + \
            '_' + summary_level[0] + '_',
            DbPrimaryKey=get_table_geo_columns(
                summary_level[0] + '_FIPS', config['geoLevelInformation'],
            ),
            DbCopyCount='1',
        ))

    return result


def get_geo_id_db_table_name(summary_level, config):
    """
    Find suffix of first table for specific geography and return it together with preceding '_'.

    :param summary_level:
    :type config: configuration file
    :return: Suffix off the first geography for that table
    """

    conn = config['connection']

    cursor = conn.cursor()

    # TODO change this ASAP, we shouldn't query the db this way
    cursor.execute(
        f"SELECT TOP 1 name FROM sys.objects WHERE type_desc = 'USER_TABLE' "
        f"and name <> 'table_names' "
        f"and left(name, 1) <> '_' "
        f"and name like '%{summary_level}%' "
        f"and name like '{config['projectId']}%' "
        f"ORDER BY name",
    )

    table_name = cursor.fetchall()

    try:
        table_name = table_name[0][0]
    except IndexError:
        logger.error(f'It looks like the database is empty!!! Exiting...')
        sys.exit()

    suffix_ind = [
        index for index, element in enumerate(
            table_name,
        ) if element == '_'
    ][-1]

    suffix = table_name[suffix_ind:]

    return suffix


def get_variables(variables, variable_description, all_guids):
    """
    Get variables for original tables
    :param variables: List of variables
    :param variable_description: List of variable descriptions
    :return:
    """
    result = []
    e = ElementMaker()

    # remove unwanted variables
    removal_vars = [
        'NAME', 'SUMLEV', 'v1', 'Geo_level',
        'QName', 'TYPE', 'GeoID', 'FIPS', 'Geo_orig',
    ]

    variables = [var for var in variables if var[0] not in removal_vars]
    variables = [var for var in variables if '_NAME' not in var[0]
                 and 'FIPS' not in var[0] and 'V1' != var[0]]

    for i in variables:
        # create variable Id's for metadata as meta_table_name + variable order

        # position after project name, summary_level, and order number, needed for variable description
        if len([index for index, character in enumerate(i[0]) if character == '_']) == 0:
            continue
        pos = [index for index, character in enumerate(i[0]) if character == '_'][1] + 1

        # create variable type
        if i[1] == 3:  # integer
            var_type = '4'
            formatting_value = '9'  # 1,234
        elif i[1] == 2:
            var_type = '7'
            formatting_value = '9'  # 1,234
        elif i[1] == 1:  # char
            var_type = '2'
            formatting_value = '0'  # none
        elif i[1] == 9 and i[2] == 3:  # none
            var_type = '7'
            formatting_value = '9'  # 1,234
        elif i[1] == 9 and i[2] == 1:
            var_type = '2'
            formatting_value = '0'  # none
        else:
            var_type = '0'
            formatting_value = '0'

        # in case unexpected variable name appear in table, logger.info warning and skip it
        # check if range exists because it searches for the variables that doesn't exists
        if i[0][pos:] not in variable_description.keys():
            logger.warning(f"Undefined variable found: {i[0]}  {i[0][pos:]}")
            continue

        variable_desc_for_retrieve = variable_description[i[0][pos:]]

        result.append(
            e.variable(  # repeated for as many times as there are variables
                GUID=all_guids.get(i[0], str(uuid.uuid4())),
                UVID='',
                PN=variable_desc_for_retrieve['palette'],
                BracketSourceVarGUID='',
                BracketFromVal='0',
                BracketToVal='0',
                BracketType='None',
                FirstInBracketSet='false',
                notes='',
                PrivateNotes='',
                name=i[0],  # meta_variable_name
                # find variable description in dictionary, by text after pr. id and
                label=variable_desc_for_retrieve['description'],
                #  order
                qLabel=variable_desc_for_retrieve['description'],
                # checks whether the indent column exist and sets the corresponding value
                indent=variable_desc_for_retrieve['indent'] if 'indent' in variable_desc_for_retrieve else '0',
                # TODO add indent info from variable desc. file
                dataType=var_type,
                dataTypeLength='0',  # default to zero
                formatting=variable_desc_for_retrieve['formatting'],
                customFormatStr='',  # only for SE tables
                FormulaFunctionBodyCSharp='',  # only for SE tables
                suppType='0',
                SuppField='',
                suppFlags='',
                aggMethod=variable_desc_for_retrieve['aggMethod'],
                DocLinksAsString='',
                #FR='COVID19Cases' if 'Cases' in variable_desc_for_retrieve['description'] else 'COVID19Deaths',
                FR=variable_desc_for_retrieve['filter_rule'],
                AggregationStr=variable_desc_for_retrieve['AggregationStr'],
                BubbleSizeHint=variable_desc_for_retrieve['bubbles']
            ),
        )
    return result


def get_tables(config, all_guids):
    """
    Get list of original tables from db
    :return: list with constructed tables tags
    """

    table_list = get_tables_from_db(config)

    table_meta_dictionary = get_table_metadata_from_db(config)
    e = ElementMaker()
    result = []
    duplication_check_list = []

    for table_name, table_variables in table_list.items():

        cut_pos = [i for i, el in enumerate(table_name) if el == '_']

        # if table is already added, skipp it
        if (table_name[:cut_pos[0]] + table_name[cut_pos[-1] + 1:]) in duplication_check_list:
            continue

        duplication_check_list.append(
            table_name[:cut_pos[0]] + table_name[cut_pos[-1] + 1:],
        )

        # this defines how table Id will be presented in metadata
        meta_table_name = table_name[:cut_pos[0]] + \
            '_' + table_name[cut_pos[-1] + 1:]
        # get last element of string after _ to be table suffix
        table_suffix = table_name.split('_')[-1]
        if table_suffix not in table_meta_dictionary.keys():
            logger.info(
                # skip unexpected table suffixes
                "Some table suffixes doesn't exist in table_names, probably autogenerated!?",
            )
            continue
        result.append(
            e.tables(
                e.table(  # get tables
                    e.OutputFormat(
                        e.Columns(

                        ),
                        TableTitle="",
                        TableUniverse="",
                    ),
                    *get_variables(
                        table_list[table_name],
                        config['variableDefinitions'], all_guids),
                    GUID=all_guids.get(meta_table_name, str(uuid.uuid4())),
                    VariablesAreExclusive='false',
                    DollarYear='0',
                    PercentBaseMin='1',
                    name=meta_table_name,
                    displayName=meta_table_name,
                    DataCategories="COVID-19;",
                    title=table_meta_dictionary[table_suffix].replace('_', ' '),
                    titleWrapped=table_meta_dictionary[table_suffix].replace('_', ' '),
                    universe='none',
                    Visible='true',
                    TreeNodeCollapsed='true',
                    CategoryPriorityOrder='0',
                    ShowOnFirstPageOfCategoryListing='false',
                    DbTableSuffix=table_suffix,
                    uniqueTableId=meta_table_name
                ),
            ),
        )

    return result


def get_geo_id_variables(geo_level_info, all_guids):
    """
    Get variables (geography identifiers) from "Geography Summary File"
    :param geo_level_info: List from config file
    :return:
    """

    nesting_information = geo_level_info
    geo_table_field_list = [
        ['QName', 'Qualifying Name', '2'], [
            'Name', 'Name of Area', '2',
        ], ['FIPS', 'FIPS', '2'],
    ]
    # create additional variables
    [
        geo_table_field_list.append([i[1].upper(), i[1], '2']) for i in
        nesting_information
    ]  # '2' means data type is string

    result = []
    e = ElementMaker()
    for i in geo_table_field_list:
        result.append(
            e.variable(
                GUID=all_guids.get(create_acronym(i[0]), str(uuid.uuid4())),
                UVID='',
                BracketSourceVarGUID='',
                BracketFromVal='0',
                BracketToVal='0',
                BracketType='None',
                FirstInBracketSet='false',
                notes='',
                PrivateNotes='',
                name=create_acronym(i[0]),
                label=i[1],
                qLabel='',
                indent='0',
                dataType=i[2],
                dataTypeLength='0',
                formatting='0',
                customFormatStr='',
                FormulaFunctionBodyCSharp='',
                suppType='0',
                SuppField='',
                suppFlags='',
                aggMethod='0',
                DocLinksAsString='',
                AggregationStr='None',
            ),
        )
    return result


def get_geo_id_tables(geo_level_info, all_guids):
    """
    Get table "Geography Identifiers" for "Geography Summary File"
    :param geo_level_info: List from config file
    :return:
    """
    # get_nesting_information = geo_level_info
    e = ElementMaker()
    result = [e.tables(
        e.table(
            e.OutputFormat(
                e.Columns(),
                TableTitle='',
                TableUniverse='',
            ),
            *get_geo_id_variables(geo_level_info, all_guids),
            GUID=all_guids.get("G001", str(uuid.uuid4())),
            VariablesAreExclusive="false",
            notes="",
            PrivateNotes="",
            DollarYear="0",
            PercentBaseMin="1",
            name="G001",
            displayName="G1.",
            title="Geography Identifiers",
            titleWrapped="Geography Identifiers",
            titleShort="",
            universe="none",
            Visible="false",
            TreeNodeCollapsed="true",
            DocSectionLinks="",
            DataCategories="",
            ProductTags="",
            FilterRuleName="",
            CategoryPriorityOrder="0",
            ShowOnFirstPageOfCategoryListing="false",
            DbTableSuffix="001",
            uniqueTableId="G001",
            source="",
            DefaultColumnCaption="",
            samplingInfo=""
        ),
    )]
    return result


# @profile
def get_project_categories(config):
    """
    Take the categories from the table definitions file
    :param config:
    :return:
    """
    if 'category' in list(config['tableDefinitions'].values())[0].keys():
        return ','.join([table['category'] for table in config['tableDefinitions'].values()])
    else:
        return ''


def create_metadata_file_structure(config, all_guids):
    e = ElementMaker()

    page = e.survey(
        e.Description(
            et.CDATA(''),
        ),
        e.notes(
            et.CDATA(''),
        ),
        e.PrivateNotes(
            et.CDATA(''),
        ),
        e.documentation(
            e.documentlinks(
            ),
            Label='Documentation',
        ),
        e.geoTypes(
            *get_geo_type(config['geoLevelInformation'], all_guids)
        ),
        e.GeoSurveyDataset(
            e.DataBibliographicInfo(

            ),
            e.notes(

            ),
            e.PrivateNotes(
                et.CDATA(''),

            ),
            e.Description(
                et.CDATA(''),

            ),
            e.datasets(
                *get_geo_survey_datasets(config, all_guids)
            ),
            e.iterations(

            ),
            *get_geo_id_tables(config['geoLevelInformation'], all_guids),
            GUID=all_guids.get('Geography Summary File', str(uuid.uuid4())),
            SurveyDatasetTreeNodeExpanded='true',
            TablesTreeNodeExpanded='true',
            IterationsTreeNodeExpanded='false',
            DatasetsTreeNodeExpanded='true',
            Description='Geographic Summary Count',
            Visible='false',
            abbreviation='Geo',
            name='Geography Summary File',
            DisplayName='Geography Summary File'
        ),
        e.SurveyDatasets(
            e.SurveyDataset(  # repeated for as many times as there are datasets
                e.DataBibliographicInfo(

                ),
                e.notes(

                ),
                e.PrivateNotes(
                    et.CDATA(''),
                ),
                e.Description(
                    et.CDATA(''),
                ),
                e.datasets(
                    *get_geo_survey_datasets(config, all_guids)
                ),
                e.iterations(

                ),
                e.tables(
                    et.Comment("Insert SE tables here !!!"),
                ),
                GUID=all_guids.get('Social Explorer Tables', str(uuid.uuid4())),
                SurveyDatasetTreeNodeExpanded='true',
                TablesTreeNodeExpanded='true',
                IterationsTreeNodeExpanded='false',
                DatasetsTreeNodeExpanded='true',
                Description='',
                Visible='false',
                abbreviation='SE',
                name='Social Explorer Tables',
                DisplayName='Social Explorer Tables',
            ),
            e.SurveyDataset(  # repeated for as many times as there are datasets
                e.DataBibliographicInfo(

                ),
                e.notes(

                ),
                e.PrivateNotes(
                    et.CDATA(''),
                ),
                e.Description(
                    et.CDATA(''),
                ),
                e.datasets(
                    *get_geo_survey_datasets(config, all_guids)
                ),
                e.iterations(

                ),
                *get_tables(config, all_guids),
                GUID=all_guids.get('Original Tables', str(uuid.uuid4())),
                SurveyDatasetTreeNodeExpanded='true',
                TablesTreeNodeExpanded='true',
                IterationsTreeNodeExpanded='false',
                DatasetsTreeNodeExpanded='true',
                Description='',
                Visible='true',
                abbreviation='ORG',
                name='Original Tables',
                DisplayName='Original Tables'
            ),
        ),
        e.Categories(
            e.string(
                # change into something more appropriate if possible
                get_project_categories(config),
            ),
        ),
        GUID=all_guids.get(config['projectId'], str(uuid.uuid4())),
        Visible='true',
        GeoTypeTreeNodeExpanded='true',
        GeoCorrespondenceTreeNodeExpanded='false',
        # metadata_file_name, changed from project_name when meaning of project name changed
        name=config['projectId'],
        DisplayName=config['projectName'],
        year=config['projectYear'],
        Categories='',
    )

    return page


def prepare_environment(config_file_path):
    """
    Initialize required variables and get information from config file
    :param config_file_path: Full path to config file
    :return:
    """

    # take values from metadata file
    config = get_config(config_file_path)

    connection_credentials = Connect(config['connectionCredentialsName'])

    if config['trustedConnection']:
        config['connection'] = connect(
            host=config['serverName'],
            database=config['databaseName'],
        )
    else:
        config['connection'] = connect(
            host=config['serverName'],
            database=config['databaseName'],
            user=connection_credentials.user_name,
            password=connection_credentials.password,
        )

    config['metadataConnectionString'] = f"Server=prime; database=; " \
                                         f"uid={connection_credentials.user_name};" \
                                         f"pwd={connection_credentials.password};" \
                                         f"Connect Timeout=1;Pooling=True"

    config['variableDefinitions'] = get_metadata_from_input_file(
        config['variableDefinitionsPath'],
    )
    config['tableDefinitions'] = get_metadata_from_input_file(
        config['tableDefinitionsPath'],
    )

    return config


def menu():
    """
    Display menu and pass command line parameters.
    :return: Options object with values from cmd
    """
    usage = "%prog -c arg"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option(
        '-c', '--config-file', dest='configFilePath', help='Full path to the config file!',
        metavar='configFilePath',
    )
    (options, args) = parser.parse_args()
    return options


def write_metadata(page, config):
    tree = et.ElementTree(page)
    tree.write(config['metadataOutputPath']+config['metadataFileName'], pretty_print=True)
    logger.info(f"Writing to: {config['metadataOutputPath']+config['metadataFileName']}")
    logger.info("Everything finished successfully!!!")


def create_metadata_file(configuration_path=r'config.yml'):
    all_guids = get_all_guids(configuration_path)
    configuration = prepare_environment(configuration_path)
    content = create_metadata_file_structure(configuration, all_guids)
    write_metadata(content, configuration)


if __name__ == '__main__':
    """
    Let's assume that config is in the same dir, TODO make it configurable
    """
    config_path = argv[1]
    create_metadata_file(config_path)
