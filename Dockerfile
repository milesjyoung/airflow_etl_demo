FROM apache/airflow:3.0.2

# Install system dependencies (optional)
USER root
RUN apt-get update && apt-get install -y libpq-dev gcc && apt-get clean

# Back to airflow user
USER airflow

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt