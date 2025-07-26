FROM bitnami/spark:3.5

WORKDIR /app

COPY ./iceberg-config /opt/bitnami/spark/conf
COPY ./extract_append.py /app/extract_append.py
COPY ./ml_extractor.py /app/ml_extractor.py

# Install dependencies
USER root
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install langchain langchain-openai pydantic pandas

USER 1001

CMD ["spark-submit", "/app/enrich_ml_batch.py"]