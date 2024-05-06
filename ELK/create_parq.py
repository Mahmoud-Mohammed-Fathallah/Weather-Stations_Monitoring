import pandas as pd
import numpy as np
import random
import time

def generate_weather_data(num_records):
    data = {
        'station_id': [random.randint(1, 10) for _ in range(num_records)],
        's_no': np.arange(1, num_records + 1),
        'battery_status': [random.choice(['low', 'medium', 'high']) for _ in range(num_records)],
        'status_timestamp': [int(time.time()) - random.randint(0, 3600*24*365) for _ in range(num_records)],
        'humidity': [random.randint(20, 80) for _ in range(num_records)],
        'temperature': [random.randint(-10, 110) for _ in range(num_records)],
        'wind_speed': [random.randint(0, 50) for _ in range(num_records)]
    }
    return data

num_records = 1000
weather_data = generate_weather_data(num_records)
df = pd.DataFrame(weather_data)

# Write the DataFrame to a CSV file
df.to_csv('weather_data.csv', index=False)

