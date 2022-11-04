import json
import time

import requests

from api_utilities import create_directory, save_data


WEATHER_METADATA = {
    'Boulder': {
        'latitude': 40.0150,
        'longitude': -105.2705,
        'grid_id': 'BOU',
        'grid_x': 53,
        'grid_y': 74
    },
    'Colorado Springs':{
        'latitude': 38.8339,
        'longitude': -104.8214,
        'grid_id': 'PUB',
        'grid_x': 89,
        'grid_y': 90
    },
    'Denver': {
        'latitude': 39.7392,
        'longitude': -104.9903,
        'grid_id': 'BOU',
        'grid_x': 62,
        'grid_y': 61  
    },
    'Grand Junction': {
        'latitude': 39.0639,
        'longitude': -108.5506,
        'grid_id': 'GJT',
        'grid_x': 94,
        'grid_y': 101
    },
    'Morrison': {
        'latitude': 39.6536,
        'longitude': -105.1911,
        'grid_id': 'BOU',
        'grid_x': 55,
        'grid_y': 57
    }
}


USER_AGENT = '(carlhinderer.com, carl.hinderer4@gmail.com)'
BASE_URL = 'https://api.weather.gov/gridpoints'
WAIT_BETWEEN_CALLS = 1

DESTINATION_DIRECTORY = '/tmp/nws'
DESTINATION_FILENAME = 'forecast.json'


def download_next_week_weather():
    try:
        create_directory(DESTINATION_DIRECTORY)
        forecasts = retrive_next_week_forecast()
        save_data(forecasts, DESTINATION_DIRECTORY, DESTINATION_FILENAME)
    except Exception as e:
        print('An exception occured: ', e)


def retrive_next_week_forecast():
    forecasts = []
    for city in WEATHER_METADATA:
        forecast = get_forecast_for_city(city)
        forecasts.insert(0, forecast)
        time.sleep(WAIT_BETWEEN_CALLS)

    return forecasts


def get_forecast_for_city(city):
    json = request_forecast_for_city(city)
    forecast_data = json['properties']['periods']
    city_forecast = {'city': city, 'forecasts': forecast_data}
    return city_forecast


def request_forecast_for_city(city):
    url = get_url(city)
    headers = {'user-agent': USER_AGENT}
    response = requests.get(url, headers=headers)
    return response.json()


def get_url(city):
    grid_id = WEATHER_METADATA[city]['grid_id']
    grid_x = WEATHER_METADATA[city]['grid_x']
    grid_y = WEATHER_METADATA[city]['grid_y']
    return f'{BASE_URL}/{grid_id}/{grid_x},{grid_y}/forecast'


if __name__ == '__main__':
    download_next_week_weather()
