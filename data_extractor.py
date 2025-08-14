
import pandas as pd
import requests
from dotenv import load_dotenv
import os





#TODO: modify to work with geo and weather and structure of their urls
def get_data(url_type, endpoint, params=None):
    """
    Get data from weather api

    Args:
        endpoint (str): endpoint you want to pull data from (meeting, session, driver, etc.)
        params (dict): optional, drill down more specifics for getting data

    Rreturn:
        pd.DataFrame: returns a data frame for easy clean and manipulation 
    """
    if params is None:
        params = {}

    url = f"{url_type}{endpoint}"
    full_url = requests.Request('GET', url, params=params).prepare().url
    response = requests.get(full_url)
    response.raise_for_status()
    return pd.DataFrame(response.json())