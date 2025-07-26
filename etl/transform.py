import pandas as pd

def transform_data(input_path="/tmp/weather_raw.pkl", output_path="/tmp/weather_transformed.pkl"):
    df = pd.read_pickle(input_path)
    daily_avg = df.groupby("date")[["temperature"]].mean().reset_index()
    daily_avg.to_pickle(output_path)