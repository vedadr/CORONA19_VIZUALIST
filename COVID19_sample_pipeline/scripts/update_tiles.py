import requests
import time
from loguru import logger
import config


# Step1 Authorization
def authorization(pw, mail):

    # Step 1: Authentication
    url = "https://api.geobuffer.com/acc/session"
    payload = "{\"email\": \""+mail+"\", \"password\": \""+pw+"\", \"authMethod\" : \"auth_header\"}\n"
    headers = {'content-type': 'application/json'}

    session_response = requests.request("POST", url, data=payload, headers=headers)
    token = session_response.headers['authorization']

    return token

# Step 2: Update State tiles
def update_state_tiles(token):
    url = "https://api.geobuffer.com/gb/job/recreate-tiles?datasetUuid={ce15ac47-df73-4fb1-8646-b5facc2bdfe1}"

    headers = {'authorization': token}

    response = requests.request("POST", url, headers=headers)
    response = response.json()

    logger.info(response.text)
    time.sleep(20)


# Step 2: Update Couty tiles
def update_county_tiles(token):
    url = "https://api.geobuffer.com/gb/job/recreate-tiles?datasetUuid={16b88478-6435-4643-bb1c-9bf149cc05e6}"

    headers = {'authorization': token}

    response = requests.request("POST", url, headers=headers)
    response = response.json()

    logger.info(response.text)
    time.sleep(20)


if __name__ == '__main__':

    email = config.Credentials().email
    password = config.Credentials().password
    ownerID = 302

    token = authorization(password, email)
    update_state_tiles(token)
    update_county_tiles(token)
