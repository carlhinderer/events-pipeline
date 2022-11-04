import json

from datetime import date, datetime, timedelta

import requests

from config import settings
from api_utilities import create_directory, save_data


API_KEY = settings.TICKETMASTER_API_KEY

BASE_URL = 'https://app.ticketmaster.com/discovery/v2/events.json'
DENVER_MARKET_ID = 6
COUNTRY_CODE = 'US'

DESTINATION_DIRECTORY = '/tmp/ticketmaster'
DESTINATION_FILENAME = 'events.json'


def download_next_week_events():
    try:
        create_directory(DESTINATION_DIRECTORY)
        events = search_denver_next_week_events()
    except Exception as e:
        print('An exception occured: ', e)


def search_denver_next_week_events():
    all_events = []
    page = 0
    num_pages = 1000
    
    while page < num_pages:
        response = requests.get(BASE_URL, params=query_params(page))
        json = response.json()
        num_pages = get_num_pages(json)
        events = get_events(json)
        all_events.extend(events)
        page = page + 1

    return all_events


def query_params(page):
    start_date, end_date = date_parameters()
    return {
        'apikey': API_KEY,
        'countryCode': COUNTRY_CODE,
        'startDateTime': str(start_date),
        'endDateTime': str(end_date),
        'marketId': DENVER_MARKET_ID,
        'page': page
    }


def date_parameters():
    start_date = date.today() + timedelta(days=1)
    start_date_time = datetime.combine(start_date, datetime.min.time())
    formatted_start = format_date_time(start_date_time)

    end_date = start_date + timedelta(days=7)
    end_date_time = datetime.combine(end_date, datetime.max.time())
    end_date_time = end_date_time.replace(microsecond=0)
    formatted_end = format_date_time(end_date_time)

    return formatted_start, formatted_end


def format_date_time(dt):
    return dt.isoformat() + 'Z'


def get_events(json_response):
    return json_response['_embedded']['events']


def get_num_pages(json_response):
    return json_response['page']['totalPages']
    

if __name__ == '__main__':
    download_next_week_events()
