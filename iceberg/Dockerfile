FROM bitnami/spark:3.5

# Set working directory
WORKDIR /app

# Copy iceberg-specific Spark config
COPY ./iceberg-config /opt/bitnami/spark/conf

# Copy the script that appends batches to Iceberg table
COPY ./append_batch.py /app/append_batches.py

# Default command — not mandatory, set by DockerOperator too
CMD ["spark-submit", "/app/append_batches.py"]