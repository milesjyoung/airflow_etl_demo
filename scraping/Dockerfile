# === Base stage: OS dependencies, Java, Playwright setup ===
FROM python:3.10-slim AS base

WORKDIR /app

# Install OS-level dependencies (Playwright, Java, fonts, etc.)
RUN apt-get update && apt-get install -y \
    wget gnupg curl unzip fonts-liberation libglib2.0-0 libnss3 libx11-xcb1 libxcb1 \
    libxcomposite1 libxdamage1 libxrandr2 libxss1 libxtst6 libgtk-3-0 \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 libdrm2 libgbm1 libxext6 libxfixes3 \
    openjdk-17-jdk \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# === Spark stage: add Spark on top of base ===
FROM base AS spark-layer

# Install Spark
ENV SPARK_VERSION=3.5.0
ENV SCALA_VERSION=2.12
ENV ICEBERG_VERSION=1.4.2

RUN curl -sL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

RUN mkdir -p /opt/spark/jars && \
    curl -L "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar" \
    -o "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar"

# Spark environment
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python


# === Final stage: copy source and install changing requirements ===
FROM spark-layer AS final

WORKDIR /app

# Copy potentially changing code and dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt
# RUN playwright install --with-deps
RUN playwright install

COPY scrape.py .

CMD ["python", "scrape.py"]