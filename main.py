"""
    Weather Data Pipeline

    use geodata api to get lat and long for list of citys
    then use that data for actual api call



"""

import pandas as pd
import os
from dotenv import load_dotenv

import data_extractor

load_dotenv()

API_KEY = os.getenv('API_KEY')
GEO_URL = os.getenv('GEODATA_API_URL')
WEATHER_URL = os.getenv('WEATHER_API_URL')

def main():
    city_dict = {'Seattle': (None, None), 'Albuquerque': (None, None)}
    weather_df = pd.DataFrame()
    #print('before', city_dict)
    for city in city_dict.keys():
        geo_df = data_extractor.get_data(GEO_URL, 'direct', {'q': city, 'appid': API_KEY})
        #print(geo_df)
        lat = geo_df.loc[0, 'lat']
        lon = geo_df.loc[0, 'lon']
        city_dict[city] = (lat, lon) 
        #print(city_dict)
   

        #TODO: save lat and long, idealy I will have a dict of city: (lat, long) so I can easy pull weather data using this 
        temp_weather_df = data_extractor.get_data(WEATHER_URL, 'weather', {'lat': city_dict[city][0], 'lon': city_dict[city][1], 'appid': API_KEY})
        #weather_df = pd.merge(temp_weather_df, geo_df, how='left', on=['lat', 'lon'])
        #print(weather_df)
        weather_df = pd.concat([weather_df, temp_weather_df], ignore_index=True)
        
    print(weather_df[['lat', 'lon', 'location', 'temperature_c']])




if __name__ == "__main__":
    main()
