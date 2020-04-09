import requests
from loguru import logger
import config


# Step1 Authorization
def authorization(pw, mail):

    # Step 1: Authentication
    url = "https://api.geobuffer.com/acc/session"
    payload = "{\"email\": \""+mail+"\", \"password\": \""+pw+"\", \"authMethod\" : \"auth_header\"}\n"
    headers = {'content-type': 'application/json'}

    logger.info("1) Creating Session...")
    session_response = requests.request("POST", url, data=payload, headers=headers)
    token = session_response.headers['authorization']

    return token


def get_columnID(token):
    url_ds = "https://api.geobuffer.com/gb/dataset/16b88478-6435-4643-bb1c-9bf149cc05e6/column"

    headers = {'authorization': token}
    dataset_columns = requests.request("GET", url_ds, headers=headers)
    columns_json = dataset_columns.json()

    columns_uuid_for_merge = []

    for i in range(0, len(columns_json)):
        column_title = columns_json[i]['title']

        if column_title == 'Geo_FIPS':
            columns_uuid_for_merge.append(columns_json[i]['uuid'])

    return columns_uuid_for_merge


if __name__ == '__main__':

    email = config.Credentials().email
    password = config.Credentials().password
    ownerID = 302
    directory = f'D:/Coronavirus data/Data for upload//'
    report_file = open('D:/Coronavirus data/datasetuuids.txt', 'w')

    token = authorization(password, email)
    column_uuid_to_merge = get_columnID(token)
