import requests
import os
import time
from datetime import datetime
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


# Step 2 Upload data sets to GB
def upload_data(token, ownerID, path, file_for_upload, collection_uuid):

    logger.info("2) Creating an upload URL...")
    # for file in range(len(files_for_upload)):
    status_completed = False
    url = "https://api.geobuffer.com/gb/job/create-upload-url"
    file_size = os.path.getsize(path + file_for_upload)

    payload = "{\r\n\t\"ownerId\":" + str(ownerID) + ",\r\n\t\"fileName\":\"" + file_for_upload + "\",\r\n\t\"contentType\":\"application/x-zip-compressed\","\
              "\r\n\t\"contentLength\":\"" + str(file_size) + "\",\r\n\t\"collectionsUuids\":[\"" + collection_uuid + "\"],\r\n\t\"ignoredFiles\":[]," \
              "\r\n\t\"sourceProjectionForFile\":\r\n\t{\r\n\t\t\"" + file_for_upload[:-4] + "/" + file_for_upload[:-4] + ".shp" + "\":\"EPSG:4326\"\r\n\t}," \
               "\r\n\t\"encodingForFile\":\r\n\t{\r\n\t\t\"" + file_for_upload[:-4] + ".csv" + "\":\"WINDOWS-1252\"\r\n\t},\r\n\t\"separatorForCsvFile\":{}\r\n}"

    headers = {
        'content-type': "application/json",
        'authorization': token
        }

    job_response = requests.request("POST", url, data=payload, headers=headers)
    job_response1 = job_response.json()
    signed_url = job_response1['signedUrl']
    job_id = job_response1['jobId']

    logger.info("3) Upload starting...")

    data = open(path + file_for_upload, 'rb')
    headers = {'content-type': 'application/x-zip-compressed'}

    response = requests.request("PUT", signed_url, data=data, headers=headers)

    url = "https://api.geobuffer.com/gb/job/" + job_id + "/complete-upload"
    headers = {
        'authorization': token,
        'content-type': "application/json"
    }
    response = requests.request("POST", url, headers=headers)

    while not status_completed:

        url = "https://api.geobuffer.com/gb/job/" + job_id + ""
        headers = {
            'authorization': token}
        response2 = requests.request("GET", url, headers=headers)
        job_response2 = response2.json()
        status = job_response2['statusCode']

        if status == 3:
            status_completed = True
            logger.info(files_for_upload[file] + " file uploading finished.")
            dataset_uuid_for_merge = job_response2['results'][0]['payload']['success']['targetDatasetUuid']

        if status == 1:
            logger.info(" -> Uploading " + file_for_upload)

        time.sleep(10)

    logger.info("Upload finished. Check your files on GeoBuffer.")
    logger.info(datetime.now())
    return dataset_uuid_for_merge


# Step 3_1: Get columns ID from Source dataset that will be imported to Target Dataset
def get_source_columnID(token, source_dataset_uuid):
    url_ds = "https://api.geobuffer.com/gb/dataset/" + source_dataset_uuid + "/column"

    headers = {'authorization': token}
    dataset_columns = requests.request("GET", url_ds, headers=headers)
    columns_json = dataset_columns.json()

    source_column_uuids = {}

    for i in range(0, len(columns_json)):
        column_title = columns_json[i]['title']

        if column_title == 'Geo_FIPS':
            source_column_uuids[columns_json[i]['title']] = columns_json[i]['uuid']

        elif not columns_json[i]['title'].startswith('Geo') and columns_json[i]['title'] != 'ID':
            source_column_uuids[columns_json[i]['title']] = columns_json[i]['uuid']

    return source_column_uuids


# Step 3_2: Get columns ID from Target dataset to compare with Source and choose variable uuids for import
def get_target_columnID(token, target_dataset_uuid):
    url_ds = "https://api.geobuffer.com/gb/dataset/" + target_dataset_uuid + "/column"

    headers = {'authorization': token}
    dataset_columns = requests.request("GET", url_ds, headers=headers)
    columns_json = dataset_columns.json()

    target_column_uuids = {}

    for i in range(0, len(columns_json)):
        column_title = columns_json[i]['title']

        if column_title == 'Geo_FIPS':
            target_column_uuids[columns_json[i]['title']] = columns_json[i]['uuid']

        elif not columns_json[i]['title'].startswith('Geo') and columns_json[i]['title'] != 'ID':
            target_column_uuids[columns_json[i]['title']] = columns_json[i]['uuid']

    return target_column_uuids


# Import columns to Targert Dataset in GB
def import_columns(token, dataset_uuid, column_uuid_to_merge, target_dataset_uuid, target_fips_uuid):
    try:

        url = "https://api.geobuffer.com/gb/dataset/" + dataset_uuid + "/evaluate-match"

        payload = "{\n\t\"sourceDatasetUuid\":\"" + target_dataset_uuid + "\",\n\t\"targetColumnUuid\":\"" + column_uuid_to_merge[-1] + "\"," \
                  "\n\t\"sourceColumnUuid\":\"" + target_fips_uuid + "\"\n}"
        headers = {
            'authorization': token,
            'content-type': "application/json"
        }

        response = requests.request("POST", url, data=payload, headers=headers)
        response_for_uuid = response.json()

        match_rows = response_for_uuid['matchedRows']
        source_rows = response_for_uuid['sourceTotalRows']

        # match rate (should be larger than 87%)
        rate = match_rows/source_rows

        if rate > 0.87:
            for i in range(0, len(column_uuid_to_merge) - 1):
                url = "https://api.geobuffer.com/gb/job/import-columns"

                payload = "{\n  \"sourceDatasetUuid\":\"" + dataset_uuid + "\",\n  " \
                          "\"targetDatasetUuid\":\"" + target_dataset_uuid + "\",\n " \
                          " \"sourceMergeKeyColumnUuid\":\"" + column_uuid_to_merge[-1] + "\",\n  " \
                          "\"targetMergeKeyColumnUuid\":\"" + target_fips_uuid + "\",\n  " \
                          "\"importColumnsUuids\":[\"" + column_uuid_to_merge[i] + "\"]\n}"

                headers = {
                    'content-type': "application/json",
                    'authorization': token
                }
                time.sleep(20)
                response = requests.request("POST", url, data=payload, headers=headers)
                logger.info(response.text)

    except Exception as e:
        logger.error(e)


if __name__ == '__main__':

    email = config.Credentials().email
    password = config.Credentials().password
    ownerID = 302
    directory = f'D:/Coronavirus data/Data for upload//'
    collection_uuid = 'afb93ea2-3d0c-42e0-a725-c085fb8520a7'

    token = authorization(password, email)

    files_for_upload = os.listdir(directory)
    for file in range(len(files_for_upload)):

        if files_for_upload[file][:-4].endswith('SL040'):
            dataset_uuid= 'ce15ac47-df73-4fb1-8646-b5facc2bdfe1'
            fips_uuid= '32c3fd7d-d206-41c6-bbc9-be351ecccf25'

        elif files_for_upload[file][:-4].endswith('SL050'):
            dataset_uuid = '16b88478-6435-4643-bb1c-9bf149cc05e6'
            fips_uuid = '350d3813-0214-4ead-8e13-c006e29a7fa0'

        dataset_for_merge_uuid = upload_data(token, ownerID, directory, files_for_upload[file], collection_uuid)

        column_uuid_to_merge = []

        target = get_target_columnID(token, dataset_uuid)
        source = get_source_columnID(token, dataset_for_merge_uuid)
        uuids_to_add = {k: source[k] for k in set(source) - set(target)}
        column_uuid_to_merge = [v for k, v in uuids_to_add.items()]

        # add FIPS uuid from source
        column_uuid_to_merge.append(source['Geo_FIPS'])

        import_columns(token, dataset_for_merge_uuid, column_uuid_to_merge, dataset_uuid, fips_uuid)
