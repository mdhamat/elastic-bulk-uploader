from elasticsearch import Elasticsearch
from typing import Any, Generator
from dotenv import load_dotenv
from elasticsearch import helpers
from pathlib import Path

import codecs


import uuid
import argparse
import os
import json
from loguru import logger

load_dotenv()

logger.add(f"./logs/elastic-run-{uuid.uuid4()}.log")


class ElasticSearchService:

    def __init__(self):
        endpoint = os.getenv("ELASTIC_ENDPOINT")
        api_key_id = os.getenv("ELASTIC_API_KEY_ID")
        api_key = os.getenv("ELASTIC_API_KEY")
        if endpoint is None:
            raise ValueError("No ElasticSearch endpoint specified")
        if api_key_id is None:
            raise ValueError("No ElasticSearch API key ID provided")
        if api_key is None:
            raise ValueError("No ElasticSearch API key provided")

        self.client = Elasticsearch(
            endpoint,
            basic_auth=(api_key_id, api_key),
            verify_certs=False,
        )

    def search(
        self,
        index: str,
        query: dict,
        size: int = 1000,
    ) -> Any:
        response = self.client.search(
            index=index,
            query=query,
            size=size,
        )
        return response

    def create_index(self, index_name: str, index_config: str) -> Any:
        with open(index_config, "r") as fh:
            index_mapping_config = json.load(fh)
        response = self.client.indices.create(
            index=index_name,
            mappings=index_mapping_config.get("mappings", None),
            settings=index_mapping_config.get("settings", None),
        )
        return response

    def does_doc_exists(self, index_name: str, query: dict) -> bool:
        res = self.client.search(
            index=index_name, query=query, size=0, source_includes=["Name"]
        )

        count = res.get("hits", {}).get("total", {}).get("value", 0)
        return count > 0

    def load_data(self, json_file_path: Path) -> dict:
        with codecs.open(str(json_file_path.resolve()), "r", "utf-8-sig") as fh:
            data = json.load(fh)
        return data

    def yield_file(self, files: list[str], index_name: str) -> Generator[Any, Any, Any]:
        for f in files:
            doc = self.load_data(Path(f))
            input_doc = {
                "_index": index_name,
                "_id": doc.get("id"),
                "_source": doc,
            }
            yield input_doc

    def index(self, doc: dict, index_name) -> None:
        self.client.index(index=index_name, id=str(uuid.uuid4()), document=doc)

    def cb(self, a):
        logger.error(a)

    def ingest_single_doc(self, path: str, index_name: str, pipeline_name: str):

        filenames = [os.path.join(path, file) for file in os.listdir(path)]
        filenames = filenames[:1]

        for doc in self.yield_file(filenames, index_name):
            index_result = self.client.index(
                index=index_name, document=doc, pipeline=pipeline_name
            )
            logger.info(f"Indexing result: {index_result}")

    def bulk_upload(
        self,
        path: str,
        index_name: str,
        pipeline_name: str,
        chunk_size: int,
        thread_count: int,
    ):
        successes = 0

        filenames = [os.path.join(path, file) for file in os.listdir(path)]

        for success, error in helpers.parallel_bulk(
            client=self.client,
            actions=self.yield_file(filenames, index_name),
            index=index_name,
            thread_count=thread_count,
            chunk_size=chunk_size,
            pipeline=pipeline_name,
            raise_on_exception=True,
            raise_on_error=True,
        ):

            logger.error(success)
            logger.error(error)
            successes += success


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Elastic Bulk Uploader")

    parser.add_argument(
        "-i",
        "--input-folder",
        required=False,
        help="Path to folder with raw JSONs",
        type=str,
    )

    parser.add_argument(
        "-a",
        "--action",
        required=True,
        help="action to perform: bulk_upload, create_index.",
        type=str,
    )

    parser.add_argument(
        "--index-name",
        required=True,
        help="Index name to perform action on.",
        type=str,
    )

    parser.add_argument(
        "--pipeline-name",
        required=False,
        help="Pipeline name to use for ingestion.",
        type=str,
    )

    parser.add_argument(
        "--chunk-size",
        required=False,
        help="Chunk size of bulk ingestion.",
        type=int,
    )

    parser.add_argument(
        "--thread-count",
        required=False,
        help="Number of threads used for ingestion.",
        type=int,
    )
    parser.add_argument(
        "--index-config",
        required=False,
        help="Index configuration for index creation.",
        type=str,
    )

    args = parser.parse_args()

    es = ElasticSearchService()

    if args.action == "create_index":
        es.create_index(args.index_name, args.index_config)
    elif args.action == "bulk_upload":
        es.bulk_upload(
            path=args.input_folder,
            index_name=args.index_name,
            pipeline_name=args.pipeline_name,
            chunk_size=args.chunk_size,
            thread_count=args.thread_count,
        )
    elif args.action == "ingest_single_doc":
        es.ingest_single_doc(
            path=args.input_folder,
            index_name=args.index_name,
            pipeline_name=args.pipeline_name,
        )
    else:
        raise ValueError(f"Unsupported action {args.action}")
