import json
import time

from datetime import date, timedelta

import requests

from config import settings
from api_utilities import create_directory


API_KEY = settings.ABSTRACT_API_KEY

BASE_URL = 'https://holidays.abstractapi.com/v1/'
COUNTRY_CODE = 'US'
TIME_BETWEEN_REQUESTS = 1

DESTINATION_DIRECTORY = '/tmp/abstract'
DESTINATION_FILENAME = 'holidays.json'


def download_next_week_holidays():
    try:
        create_directory(DESTINATION_DIRECTORY)
        holidays = retrieve_next_week_holidays()
        save_holidays(holidays)
    except Exception as e:
        print('An exception occured: ', e)


def retrieve_next_week_holidays():
    day_to_retrieve = date.today()
    holidays = []

    for i in range(7):
        day_to_retrieve += timedelta(days=1)
        holiday_data = get_holidays_for_date(day_to_retrieve)
        holidays.append(holiday_data)
        time.sleep(TIME_BETWEEN_REQUESTS)

    return holidays


def get_holidays_for_date(day_to_retrieve):
    json = request_holidays_for_date(day_to_retrieve)
    holiday_key = str(day_to_retrieve)
    holiday_names = [holiday['name'] for holiday in json]
    return {holiday_key: holiday_names}


def request_holidays_for_date(day_to_retrieve):
    query_params = get_query_params(day_to_retrieve)
    response = requests.get(BASE_URL, params=query_params)
    return response.json()


def get_query_params(day_to_retrieve):
    return {
        'api_key': API_KEY,
        'country': COUNTRY_CODE,
        'year': day_to_retrieve.year,
        'month': day_to_retrieve.month,
        'day': day_to_retrieve.day
    }


def save_holidays(holidays):
    file_path = os.path.join(DESTINATION_DIRECTORY, DESTINATION_FILENAME)
    with open(file_path, 'w') as data_file:
        json.dump(holidays, data_file)


if __name__ == '__main__':
    download_next_week_holidays()
