import json
import os
from datetime import datetime, timezone

import requests
from google.api_core.exceptions import NotFound, AlreadyExists
from google.cloud import secretmanager_v1, bigquery

gcp_project = os.environ.get('GCP_PROJECT', None)

strava_based_url = os.environ.get('STRAVA_BASED_URL', 'https://www.strava.com')
client_id = os.environ.get('STRAVA_CLIENT_ID', None)
client_secret = os.environ.get('STRAVA_CLIENT_SECRET', None)
client_code = os.environ.get('STRAVA_CLIENT_CODE', None)

bq_dataset = os.environ.get('INGESTION_DATA_SET', None)
bq_table = os.environ.get('INGESTION_TABLE', None)
bq_table_id = f"{gcp_project}.{bq_dataset}.{bq_table}"

# client initiation
client_secret_manager = secretmanager_v1.SecretManagerServiceClient()
client_bq = bigquery.Client()


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
        response = client_secret_manager.access_secret_version(request=request)
        return response.payload.data.decode("utf-8")
    except NotFound as e:
        print(f'Secret {secret_id} not found')
        return None


def create_secret(secret_id, secret_value):
    try:
        response = client_secret_manager.create_secret(
            request={
                "parent": f"projects/{gcp_project}",
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}},
            }
        )
        print(f"Created secret: {response.name}")
    except AlreadyExists as e:
        print(f"The secret {secret_id} already exists")

    payload = str(secret_value).encode("UTF-8")
    response = client_secret_manager.add_secret_version(
        request={
            "parent": client_secret_manager.secret_path(gcp_project, secret_id),
            "payload": {"data": payload}
        }
    )

    print(f"Added secret version: {response.name}")


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
        expires_at = datetime.fromtimestamp(int(auth_details.expires_at)).replace(tzinfo=timezone.utc)
        if expires_at.timestamp() < datetime.now(expires_at.tzinfo).timestamp():
            url = f"{strava_based_url}/api/v3/oauth/token?client_id={client_id}&client_secret={client_secret}" \
                  f"&grant_type=refresh_token&refresh_token={auth_details.refresh_token}"

            response = requests.request("POST", url)
            auth_details = AuthDetails(response.json()['expires_at'], response.json()['access_token'],
                                       response.json()['refresh_token'])
            save_auth_details(auth_details)

    return auth_details.access_token


def created_time_limit_query():
    query_job = client_bq.query(f'SELECT start_date FROM `{bq_table_id}` ORDER BY start_date DESC LIMIT 1 ')

    rows = query_job.result()
    rows_list = list(rows)

    last_fetched_time = None
    if rows_list:
        row = rows_list[0]
        last_fetched_time = row.start_date.replace(tzinfo=timezone.utc)
    else:
        last_fetched_time = datetime(2010, 1, 1, tzinfo=timezone.utc)

    return str(last_fetched_time.timestamp())


def get_latest_activities():
    time_limit_query = created_time_limit_query()
    url = f"{strava_based_url}/api/v3/athlete/activities?after={time_limit_query}&per_page=200"
    auth_token = get_auth_token()
    headers = {
        'Authorization': f'Bearer {auth_token}'
    }
    response = requests.request("GET", url, headers=headers)
    return json.loads(response.text)


def process(activities):
    for activity in activities:
        persist_activity(activity)


def if_table_exists(table_name):
    #TODO fix deprecation
    dataset = client_bq.dataset(bq_dataset, gcp_project)
    table_ref = dataset.table(table_name)
    try:
        client_bq.get_table(table_ref)
        return True
    except NotFound:
        return False


def persist_activity(activity):
    start_date_local = datetime.strptime(activity['start_date_local'], '%Y-%m-%dT%H:%M:%SZ')
    start_date = datetime.strptime(activity['start_date'], '%Y-%m-%dT%H:%M:%SZ')
    ingestion_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    id = activity['id']

    row_to_insert = {
        "ingestion_time": str(ingestion_time.timestamp()),
        "name": activity['name'],
        "distance": activity['distance'],
        "moving_time": activity['moving_time'],
        "elapsed_time": activity['elapsed_time'],
        "total_elevation_gain": activity['total_elevation_gain'],
        "type": activity['type'],
        "sport_type": activity['sport_type'],
        "id": id,
        "start_date": str(start_date),
        "start_date_local": str(start_date_local),
        "timezone": activity['timezone'],
        "utc_offset": activity['utc_offset'],
        "kudos_count": activity['kudos_count'],
        "comment_count": activity['comment_count'],
        "visibility": activity['visibility'],
        "start_lat": None if len(activity['start_latlng']) == 0 else activity['start_latlng'][0],
        "start_lng": None if len(activity['start_latlng']) == 0 else activity['start_latlng'][1],
        "end_lat": None if len(activity['end_latlng']) == 0 else activity['end_latlng'][0],
        "end_lng": None if len(activity['end_latlng']) == 0 else activity['end_latlng'][1],
        "average_speed": activity['average_speed'],
        "max_speed": activity['max_speed'],
        "elev_high": None if "elev_high" not in activity else activity['elev_high'],
        "elev_low": None if "elev_low" not in activity else activity['elev_low'],
        "total_photo_count": activity['total_photo_count']
    }

    errors = client_bq.insert_rows_json(bq_table_id, [row_to_insert])
    if not errors:
        print(f"New rows have been added with id {id} and ingestion time {ingestion_time}.")
    else:
        print(f"Encountered errors while inserting rows with {id} and {ingestion_time} = {errors})")


def create_table_if_not_exists():
    schema = [
        bigquery.SchemaField("ingestion_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("distance", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("moving_time", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("elapsed_time", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("total_elevation_gain", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sport_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("start_date", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("start_date_local", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("timezone", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("utc_offset", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("kudos_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("comment_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("visibility", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("start_lat", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("start_lng", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("end_lat", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("end_lng", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("average_speed", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("max_speed", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("elev_high", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("elev_low", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("total_photo_count", "INTEGER", mode="REQUIRED")
    ]

    if not if_table_exists(bq_table):
        table = bigquery.Table(bq_table_id, schema=schema)
        table = client_bq.create_table(table)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingestion_time",
            expiration_ms=7776000000,
        )
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
    else:
        print("Table already exits, will not be recreated.")


def init(data=None, context=None):
    create_table_if_not_exists()
    activities = get_latest_activities()
    process(activities)


if __name__ == '__main__':
    init()
