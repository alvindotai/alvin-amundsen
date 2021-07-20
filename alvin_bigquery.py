# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script for extracting BigQuery usage results
"""

import logging
import os

from databuilder.extractor.bigquery_metadata_extractor import BigQueryMetadataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import ChainedTransformer, NoopTransformer
from dotenv import load_dotenv
from pyhocon import ConfigFactory

from alvin_transformer import AlvinTransformer

load_dotenv("./credentials.env")

logging.basicConfig(
    filename="./error.log",
    level=logging.NOTSET
)

# set env NEO4J_HOST to override localhost
NEO4J_ENDPOINT = f'bolt://{os.getenv("NEO4J_HOST", "localhost")}:7687'
neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'

# ALVIN CONFIG VARS
alvin_key = os.getenv('ALVIN_API_KEY')
alvin_platform_id = os.getenv('ALVIN_BIGQUERY_PLATFORM_ID')
alvin_instance_url = os.getenv("ALVIN_INSTANCE_URL")
alvin_platform_type = "bigquery"
alvin_tableau_site_name = os.getenv("TABLEAU_SITE_NAME")


# todo: Add a second model
def create_bq_job(metadata_type, gcloud_project):
    tmp_folder = f'/var/tmp/amundsen/{metadata_type}'
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    bq_meta_extractor = BigQueryMetadataExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=bq_meta_extractor,
                       loader=csv_loader,
                       transformer=ChainedTransformer(
                           transformers=[
                               NoopTransformer(),
                               AlvinTransformer()
                           ],
                           is_init_transformers=True
                       )
                       )

    job_config = ConfigFactory.from_dict({
        f'extractor.bigquery_table_metadata.{BigQueryMetadataExtractor.PROJECT_ID_KEY}': gcloud_project,
        "transformer.chained.transformer.alvin_transformer.platform_id": alvin_platform_id,
        "transformer.chained.transformer.alvin_transformer.platform_type": alvin_platform_type,
        "transformer.chained.transformer.alvin_transformer.api_key": alvin_key,
        "transformer.chained.transformer.alvin_transformer.alvin_instance_url": alvin_instance_url,
        "transformer.chained.transformer.alvin_transformer.tableau_site_name": alvin_tableau_site_name,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR}': True,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4j_endpoint,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4j_user,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4j_password,
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    return job


if __name__ == "__main__":
    bq_job = create_bq_job('bigquery_metadata', 'your-project-here')
    bq_job.launch()
