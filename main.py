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
   df = data_extractor.get_data(GEO_URL, 'direct', {'q': 'Seattle', 'appid': API_KEY})
   print(df)

   #TODO: save lat and long, idealy I will have a dict of city: (lat, long) so I can easy pull weather data using this 





if __name__ == "__main__":
    main()
