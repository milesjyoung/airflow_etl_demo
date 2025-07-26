FROM bitnami/spark:3.5

# Set working directory
WORKDIR /app

# Install Node.js and dependencies for Playwright
USER root
RUN install_packages curl ca-certificates gnupg python3-pip wget \
    && curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && pip install --no-cache-dir playwright pandas \
    && playwright install --with-deps

# Copy iceberg-specific Spark config
COPY ./iceberg-config /opt/bitnami/spark/conf

# Copy starting Master list of records (dont do this in production just run a one time script)
COPY ./MasterLicenseData.csv /extras/MasterLicenseData.csv

# Copy the script that appends batches to Iceberg table
COPY ./installer_scraper.py /app/installer_scraper.py
COPY ./match_license.py /app/match_license.py

# Default command — not mandatory, set by DockerOperator too
CMD ["spark-submit", "/app/append_batches.py"]