"""
    Weather Data Pipeline

    use geodata api to get lat and long for list of citys
    then use that data for actual api call



"""

import pandas as pd
import os
from dotenv import load_dotenv
import requests

import data_extractor as data_extractor

load_dotenv()

API_KEY = os.getenv('API_KEY')
GEO_URL = os.getenv('GEODATA_API_URL')
WEATHER_URL = os.getenv('WEATHER_API_URL')




def api_data_pull(citys, weather_df, geo_df):
    for city in citys.keys():
        temp_geo_df = data_extractor.get_data(GEO_URL, 'direct', {'q': city, 'appid': API_KEY})
        geo_df = pd.concat([geo_df, temp_geo_df], ignore_index=True)
        #print(geo_df)
        lat = temp_geo_df.loc[0, 'lat']
        lon = temp_geo_df.loc[0, 'lon']
        citys[city] = (lat, lon) 
        #print(city_dict)

        #TODO: save lat and long, idealy I will have a dict of city: (lat, long) so I can easy pull weather data using this 
        temp_weather_df = data_extractor.get_data(WEATHER_URL, 'weather', {'lat': citys[city][0], 'lon': citys[city][1], 'appid': API_KEY})
        #weather_df = pd.merge(temp_weather_df, geo_df, how='left', on=['lat', 'lon'])
        #print(weather_df)
        weather_df = pd.concat([weather_df, temp_weather_df], ignore_index=True)
    return weather_df, geo_df


def test():
    with open("my_file.txt", "a") as file:
    # Write the new content to the end of the file
        file.write("test\n")



def main():
    city_dict = {'Seattle': (None, None),
                 'Albuquerque': (None, None), 
                 'New York': (None, None), 
                 'Beijing': (None, None),
                 'Fukuoka': (None, None)}
    weather_df = pd.DataFrame()
    geo_df = pd.DataFrame()
    weather_df, geo_df = api_data_pull(city_dict, weather_df, geo_df)
    test() 
    print(weather_df[['lat', 'lon', 'location', 'temperature_c']])
        # Check if the file exists to determine whether to write the header
    file_path = 'test.csv'
    if not os.path.isfile(file_path):
        # If the file doesn't exist, create it and write the header
        weather_df.to_csv(file_path, mode='a', index=False, header=True)
    else:
        # If the file exists, append without writing the header again
        weather_df.to_csv(file_path, mode='a', index=False, header=False)
    #print(geo_df)
    






if __name__ == "__main__":
    main()
