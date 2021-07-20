# Alvin Connector With Amundsen

Use `alvin_transformer.py` with your existing Amundsen ETL jobs to extract table and column lineage data from your BigQuery instance in Alvin. You will need an API key, your Alvin platform instance ID, and the URL where your Alvin instance can be found.

Use the `alvin_bigquery.py` file with your BigQuery configuration to test the ingestion of lineage data into your Amundsen instance. Make sure you have the right BigQuery credentials and permissions set.

Use the `alvin_tableau.py` file with your Tableau configurataion to ingest dashboard metadata into your Amundsen instance. You'll need to know your secret token (found in your Tableau settings), website name, base url, API url, and API version.

#### Note:
All of these scripts use the `dotenv` python package to load environment variables from a file. Currently, that file is set as `credentials.env`, which for obvious reasons is not included in the repository. Feel free to add your own `credentials.env` file when you pull down this repository, as you'll need it for running the scripts.
