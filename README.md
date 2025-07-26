# ETL pipeline demo with Airflow

This project is a demo of a full stack ETL pipeline. The setup is currently for local testing, but can easily be moved to a cloud environment as all tasks are containerized for batch jobs. 

The scraper uses Playwright to extract solar permits from Nevada County and scrape the site for specific details including installer CSLB numbers. There are a few examples in the code of how you can handle data transfer, but the current setup dumps batches of parquet formatted files to storage (you can configure where).

The iceberg append script will process raw permit info dumps and append to an iceberg formatted data lake.

The iceberg with playwright attempts to match the new permits CSLBs to installer business names in an existing iceberg table, however it will attempt to scrape the name from the California CSLB registry if the entry is not found in the existing list.

Finally, the permit ML workflow processes the unstructured descriptions (using langchain/OpenAI models) attempting to extract import details about the particular solar project corresponding to the permit and appends these results to another iceberg table.

The structure of the demo is created through a modified docker-compose (very close to the starter provided by airflow) launching a cluster of services. All example DAGs are in the dag folder.

## Steps

* First build all the necessary containers (look in the dag for corresponding container names).
* Start by running the airflow init service with docker compose
* Proceed to start the services
* unpause any existing DAGs and you should be able to view the results in the browser (service included in compose file)