"""Carrega no Amundsen os modelos do dbt: tabelas, colunas, descrições e linhagem.

Lê o manifest.json e o catalog.json gerados por `dbt docs generate` (tarefa `dbt_docs`
do DAG), publica no Neo4j e indexa no Elasticsearch para a busca.
"""

import os
import sys
import uuid
from pathlib import Path

from databuilder.extractor.dbt_extractor import DbtExtractor
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.search.search_metadata_to_elasticsearch_task import (
    SearchMetadatatoElasticasearchTask,
)
from databuilder.task.task import DefaultTask
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory

ARTIFACTS = Path(os.environ.get("DBT_ARTIFACTS_DIR", "/dbt-artifacts"))
NEO4J_ENDPOINT = os.environ.get("NEO4J_ENDPOINT", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
ELASTICSEARCH_URL = os.environ.get("ELASTICSEARCH_URL", "http://elasticsearch:9200")
# No Amundsen, "database" é o tipo da fonte; o banco em si vira o "cluster".
SOURCE_TYPE = "postgres"
TMP = Path("/tmp/amundsen")


def publish_dbt_metadata() -> None:
    """dbt (manifest + catalog) -> arquivos CSV -> Neo4j."""
    nodes, relations = TMP / "nodes", TMP / "relationships"
    conf = ConfigFactory.from_dict(
        {
            f"extractor.dbt.{DbtExtractor.DATABASE_NAME}": SOURCE_TYPE,
            f"extractor.dbt.{DbtExtractor.MANIFEST_JSON}": str(ARTIFACTS / "manifest.json"),
            f"extractor.dbt.{DbtExtractor.CATALOG_JSON}": str(ARTIFACTS / "catalog.json"),
            f"loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}": str(nodes),
            f"loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}": str(relations),
            f"loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR}": True,
            f"publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}": str(nodes),
            f"publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}": str(relations),
            f"publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}": NEO4J_ENDPOINT,
            f"publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}": NEO4J_USER,
            f"publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}": NEO4J_PASSWORD,
            f"publisher.neo4j.{neo4j_csv_publisher.NEO4J_ENCRYPTED}": False,
            f"publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}": uuid.uuid4().hex,
        }
    )
    task = DefaultTask(extractor=DbtExtractor(), loader=FsNeo4jCSVLoader())
    DefaultJob(conf=conf, task=task, publisher=Neo4jCsvPublisher()).launch()


def index_for_search(entity: str = "table") -> None:
    """Neo4j -> índice de busca no Elasticsearch."""
    task = SearchMetadatatoElasticasearchTask(extractor=Neo4jSearchDataExtractor())
    task_scope = task.get_scope()
    neo4j = "extractor.search_data.extractor.neo4j.{}".format
    conf = ConfigFactory.from_dict(
        {
            f"{task_scope}.{SearchMetadatatoElasticasearchTask.ENTITY_TYPE}": entity,
            f"{task_scope}.{SearchMetadatatoElasticasearchTask.ELASTICSEARCH_CLIENT_CONFIG_KEY}": (
                Elasticsearch(ELASTICSEARCH_URL)
            ),
            f"{task_scope}.{SearchMetadatatoElasticasearchTask.ELASTICSEARCH_ALIAS_CONFIG_KEY}": (
                # apelido que o serviço de busca do Amundsen (proxy v2.1) procura
                f"{entity}_search_index_v2_1"
            ),
            f"extractor.search_data.{Neo4jSearchDataExtractor.ENTITY_TYPE}": entity,
            neo4j(Neo4jExtractor.GRAPH_URL_CONFIG_KEY): NEO4J_ENDPOINT,
            neo4j(Neo4jExtractor.NEO4J_AUTH_USER): NEO4J_USER,
            neo4j(Neo4jExtractor.NEO4J_AUTH_PW): NEO4J_PASSWORD,
            neo4j(Neo4jExtractor.NEO4J_ENCRYPTED): False,
        }
    )
    DefaultJob(conf=conf, task=task).launch()


def main() -> int:
    missing = [f for f in ("manifest.json", "catalog.json") if not (ARTIFACTS / f).exists()]
    if missing:
        print(
            f"Faltam {missing} em {ARTIFACTS}. Rode o DAG mercado_trabalho_elt antes "
            "(a tarefa dbt_docs gera esses arquivos).",
            file=sys.stderr,
        )
        return 1
    publish_dbt_metadata()
    index_for_search("table")
    print("Catálogo carregado no Amundsen.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
