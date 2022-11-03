import json
import os
import shutil

from pathlib import Path

import requests


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

DESTINATION_DIRECTORY = '/tmp/nws'
DESTINATION_FILENAME = 'forecast.json'
