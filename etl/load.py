import pandas as pd
import psycopg2

def load_to_postgres(input_path="/tmp/weather_transformed.pkl"):
    df = pd.read_pickle(input_path)
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_weather (
            date DATE PRIMARY KEY,
            avg_temperature FLOAT
        );
    """)
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO daily_weather (date, avg_temperature)
            VALUES (%s, %s)
            ON CONFLICT (date) DO UPDATE SET avg_temperature = EXCLUDED.avg_temperature;
        """, (row['date'], row['temperature']))
    conn.commit()
    cursor.close()
    conn.close()
