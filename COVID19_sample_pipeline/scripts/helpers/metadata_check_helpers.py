from loguru import logger


def get_input_data():

    try:
        xml_path = input("Please enter path to xml file: ")
    except OSError:
        logger.error('No such file!')
        exit()

    if_new_xsd_schema = input("Please enter YES if you have a new xsd schema if available, else type in 'NO': ")

    if if_new_xsd_schema == 'YES' or if_new_xsd_schema == 'yes' or if_new_xsd_schema == 'Y' or if_new_xsd_schema == 'y':
        try:
            xsd_path = input("Please enter path to new xsd schema file: ")
        except OSError:
            logger.error('No such file!')
            exit()
    else:
        xsd_path = '../tests/res/xml_to_be_checked/correct_schema.xsd'

    try:
        all_xml_path = input("Please enter path to all xml files in the system: ")
    except OSError:
        logger.error('No such path!')
        exit()

    return all_xml_path, xml_path, xsd_path
