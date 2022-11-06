import json
import requests
import datetime
import os.path
import os


strava_based_url = os.environ.get('STRAVA_BASED_URL', 'https://www.strava.com')
client_id = os.environ.get('STRAVA_CLIENT_ID', None)
client_secret = os.environ.get('STRAVA_CLIENT_SECRET', None)
client_code = os.environ.get('STRAVA_CLIENT_CODE', None)


def get_auth_token():
    if os.path.isfile('tokens_cache.json'):
        with open("tokens_cache.json", "r") as f:
            json_object = json.load(f)
            expires_at = datetime.datetime.utcfromtimestamp(json_object['expires_at'])
            if expires_at.timestamp() > datetime.datetime.now(expires_at.tzinfo).timestamp():
                return json_object['access_token']
            else:
                url = f"{strava_based_url}/api/v3/oauth/token?client_id={client_id}&client_secret={client_secret}" \
                      f"&grant_type=refresh_token&refresh_token={json_object['refresh_token']}"

                response = requests.request("POST", url)
                json_object['refresh_token'] = response.json()['refresh_token']
                json_object['access_token'] = response.json()['access_token']
                json_object['expires_at'] = response.json()['expires_at']
                with open("tokens_cache.json", "w") as outfile:
                    to_be_written = json.dumps(json_object, indent=4)
                    outfile.write(to_be_written)
                return json_object['access_token']

    url = f"{strava_based_url}/oauth/token?client_id={client_id}&client_secret={client_secret}" \
          f"&code={client_code}&grant_type=authorization_code"
    response = requests.request("POST", url)
    json_object = json.dumps(response.json(), indent=4)
    with open("tokens_cache.json", "w") as outfile:
        outfile.write(json_object)
    return response.json()['access_token']


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
