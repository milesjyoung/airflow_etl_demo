import pandas as pd

def extract_csv(input_path="/opt/airflow/data/raw_weather.csv", output_path="/tmp/weather_raw.pkl"):
    df = pd.read_csv(input_path)
    df.to_pickle(output_path)
