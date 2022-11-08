import datetime
import json
import os

import requests
from google.api_core.exceptions import NotFound, AlreadyExists
from google.cloud import secretmanager_v1

strava_based_url = os.environ.get('STRAVA_BASED_URL', 'https://www.strava.com')
client_id = os.environ.get('STRAVA_CLIENT_ID', None)
client_secret = os.environ.get('STRAVA_CLIENT_SECRET', None)
client_code = os.environ.get('STRAVA_CLIENT_CODE', None)
gcp_project = os.environ.get('GCP_PROJECT', None)

client = secretmanager_v1.SecretManagerServiceClient()

class AuthDetails:
    expires_at = None
    access_token = None
    refresh_token = None

    def __init__(self, expires_at, access_token, refresh_token):
        self.expires_at = expires_at
        self.access_token = access_token
        self.refresh_token = refresh_token


def get_auth_details():
    expires_at = get_secret("expires_at")
    access_token = get_secret("access_token")
    refresh_token = get_secret("refresh_token")
    return AuthDetails(expires_at, access_token, refresh_token)


def get_secret(secret_id):
    request = secretmanager_v1.AccessSecretVersionRequest(
        name=f'projects/{gcp_project}/secrets/{secret_id}/versions/latest'
    )

    try:
        response = client.access_secret_version(request=request)
        return response.payload.data.decode("utf-8")
    except NotFound as e:
        print(f'Secret {secret_id} not found')
        return None


def create_secret(secret_id, secret_value):
    try:
        response = client.create_secret(
            request={
                "parent": f"projects/{gcp_project}",
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}},
            }
        )
        print("Created secret: {}".format(response.name))
    except AlreadyExists as e:
        print("The secret {} already exists", secret_id)

    payload = str(secret_value).encode("UTF-8")
    response = client.add_secret_version(
        request={
            "parent": client.secret_path(gcp_project, secret_id),
            "payload": {"data": payload}
        }
    )

    print("Added secret version: {}".format(response.name))


def save_auth_details(auth_details):
    create_secret("expires_at", auth_details.expires_at)
    create_secret("access_token", auth_details.access_token)
    create_secret("refresh_token", auth_details.refresh_token)


def get_auth_token():
    auth_details = get_auth_details()
    if auth_details.expires_at is None:
        url = f"{strava_based_url}/oauth/token?client_id={client_id}&client_secret={client_secret}" \
              f"&code={client_code}&grant_type=authorization_code"
        response = requests.request("POST", url)
        auth_details = AuthDetails(response.json()['expires_at'], response.json()['access_token'],
                                   response.json()['refresh_token'])
        save_auth_details(auth_details)
    else:
        expires_at = datetime.datetime.utcfromtimestamp(int(auth_details.expires_at))
        if expires_at.timestamp() < datetime.datetime.now(expires_at.tzinfo).timestamp():
            url = f"{strava_based_url}/api/v3/oauth/token?client_id={client_id}&client_secret={client_secret}" \
                  f"&grant_type=refresh_token&refresh_token={auth_details.refresh_token}"

            response = requests.request("POST", url)
            auth_details = AuthDetails(response.json()['expires_at'], response.json()['access_token'],
                                       response.json()['refresh_token'])
            save_auth_details(auth_details)

    return auth_details.access_token


def created_time_limit_query(year):
    before = datetime.datetime(year, 12, 31, 0, 0).strftime('%s')
    after = datetime.datetime(year, 1, 1, 0, 0).strftime('%s')

    return f'before={before}&after={after}'


def get_activities(year):
    time_limit_query = created_time_limit_query(year)
    url = f"{strava_based_url}/api/v3/athlete/activities?{time_limit_query}&per_page=200"
    auth_token = get_auth_token()
    headers = {
        'Authorization': f'Bearer {auth_token}'
    }
    response = requests.request("GET", url, headers=headers)
    return response.text


def extract_stats(activities_json, year):
    data = json.loads(activities_json)
    total_distance = 0
    total_elevation_gain = 0
    total_moving_time = 0
    count_activities = 0
    for i in data:
        if i['type'] == 'Ride':
            total_distance += i['distance']
            total_elevation_gain += i['total_elevation_gain']
            total_moving_time += i['moving_time']
            count_activities += 1
    print('--------------')
    print(f"for year {year}")
    print(f"total activities {count_activities}")
    print('total distance')
    print(("%.2f km" % (total_distance / 1000)))
    print('total elevation gain')
    print(("%.2f km" % (total_elevation_gain / 1000)))
    print('total moving time')
    print(f"{str(datetime.timedelta(seconds=total_moving_time))}")


def process_stats(starting_year, ending_year):
    for year in range(starting_year, ending_year):
        json_data = get_activities(year)
        extract_stats(json_data, year)


if __name__ == '__main__':
    process_stats(2017, 2023)
