#%% LIB
import requests
from dotenv import load_dotenv
import os
from datetime import datetime, timezone
import sqlite3

load_dotenv()
#%% EXTRACT

def extract(city, api_key):

    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    response = requests.get(url)
    data = response.json()
    
    return data

#%% TRANSFORM

def transform(city, data):
    return {
    "city": city,
    "date": datetime.fromtimestamp(data['dt'], timezone.utc),
    "weather": data['weather'][0]['description'],
    "temp_celcius": data['main']['temp']-273.15,
    "humidity_percent": data['main']['humidity'],
    "cloudiness_percent": data['clouds']['all']
    }

#%% LOAD

def load(weather_data):

    conn = sqlite3.connect('weather.db')
    cursor = conn.cursor()
    
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS weather_data(
                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                       city TEXT,
                       date TIMESTAMP,
                       weather TEXT,
                       temP_celcius REAL,
                       humidity_percent REAL,
                       cloudiness_percent REAL                
                       )
                   
                   """)
    
    cursor.execute("""
                   INSERT INTO weather_data (city, date, weather, temp_celcius, humidity_percent, cloudiness_percent)
                   VALUES (?, ?, ?, ?, ?, ?)
                   """,(
                   weather_data['city'],
                   weather_data['date'],
                   weather_data['weather'],
                   weather_data['temp_celcius'],
                   weather_data['humidity_percent'],
                   weather_data['cloudiness_percent']
                   )
                   )
    
    conn.commit()
    conn.close()
    
#%% MAIN PIPELINE FUNCTION
def run_etl():
    api_key = os.getenv("SECRETE_KEY")
    city = 'Hanoi'
    
    raw_data = extract(city, api_key)
    cleaned_data = transform(city, raw_data)
    load(cleaned_data)
    
if __name__ == "__main__":
    run_etl()

