
import pandas as pd
import requests
from dotenv import load_dotenv
import os





#TODO: modify to work with geo and weather and structure of their urls
def get_data(url_type, endpoint, params=None):
    """
    Get data from weather api

    Args:
        url_type (str): api url
        endpoint (str): endpoint to use with the api url
        params (dict): params to pass with api call

    Rreturn:
        pd.DataFrame: returns a data frame for easy clean and manipulation 
    """
    if params is None:
        params = {}

    url = f"{url_type}{endpoint}"
    full_url = requests.Request('GET', url, params=params).prepare().url
    response = requests.get(full_url)
    if endpoint == 'weather':
        return weather_data(response.json())
    
    response.raise_for_status()
    return pd.DataFrame(response.json())


# Clean up weather json to make work with pandas dataFrame
def weather_data(json):
    df = pd.json_normalize(json,
                            'weather',
                            [['main', 'temp'],
                            ['main', 'feels_like'],
                            ['main', 'temp_min'],
                            ['main', 'temp_max'],
                            ['main', 'pressure'],
                            ['main', 'humidity'],
                            'visibility', 
                            ['wind', 'speed'],
                            ['wind', 'deg'],
                            'dt',
                            'timezone',
                            'name',
                            ['coord','lat'],
                            ['coord','lon']])
    df = data_clean(df)
    #print(df.columns)

    
    #print(df.dtypes)
    return df

def data_clean(df):
    df = df.rename(columns={'id': 'weather_id',
               'main':'weather',
               'description':'weather_description',
               'main.temp':'temperature',
               'main.feels_like':'temperature_feels_like',
               'main.temp_min':'minimum_temperature',
               'main.temp_max':'maximum_temperature',
               'main.pressure':'pressure',
               'main.humidity':'humidity',
               'wind.speed':'wind_speed',
               'wind.deg':'wind_direction',
               'dt':'date_time_unix_utc',
               'timezone':'timezone_offset',
               'name': 'location',
               'coord.lat': 'lat',
               'coord.lon':'lon'})
    df = df.convert_dtypes().copy()
    df['date_time_unix_local'] = df['date_time_unix_utc'] + df['timezone_offset']
    df['temperature_c'] = df['temperature'] - 273.15
    df['temperature_feels_like_c'] = df['temperature_feels_like'] - 273.15
    df['minimum_temperature_c'] = df['minimum_temperature'] - 273.15
    df['maximum_temperature_c'] = df['maximum_temperature'] - 273.15

    return df



