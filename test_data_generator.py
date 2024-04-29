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
import ipsum
import random
import datetime

ipsum_model = ipsum.load_model("en")


def random_date():
    start = datetime.datetime(2020, 1, 1)
    end = datetime.datetime(2023, 12, 31)
    diff = end - start
    random_seconds = diff.total_seconds() * random.random()
    random_timedelta = datetime.timedelta(seconds=random_seconds)
    return start + random_timedelta


def get_format(format_string: str):
    random_date_obj = random_date()
    random_date_str = random_date_obj.strftime(format_string)
    return random_date_str


def get_ipc_cpc() -> str:
    return random.choice(
        [
            "A23L3/30",
            "A47B9/00",
            "A61B5/00",
            "A61B17/00",
            "A61B19/00",
            "A61B25/00",
            "A61B27/00",
            "A61B30/00",
            "A61B31/00",
            "A61B32/00",
            "A61B33/00",
            "A61B5/11",
            "A61B5/14",
            "A61B5/16",
            "A61B5/22",
            "A61B5/37",
            "A61B5/41",
            "A61B5/44",
            "A61B5/45",
            "A61B5/46",
            "A61B5/47",
            "A61B5/48",
            "A61B5/49",
            "A61B5/50",
            "A61B5/51",
            "A61B5/52",
            "A61B5/53",
            "A61B5/54",
            "A61B5/55",
            "A61B5/56",
            "A61B5/58",
            "A61B5/59",
            "A61B5/60",
            "A61B5/61",
            "A61B5/62",
            "A61B5/63",
            "A61B5/64",
            "A61B5/65",
            "A61B5/66",
            "A61B5/67",
            "A61B5/68",
        ]
    )


def generate_patent_text() -> str:
    text = " ".join(ipsum_model.generate_paragraphs(2))
    return text.encode("ascii", "ignore").decode()


def yield_sample() -> dict:
    start = random.randint(0, 512)
    end = start + random.randint(0, 512)
    pat_num = random.randint(1000000, 9000000)
    pub_id = random.randint(10000000, 90000000)

    country = random.choice(["EP", "US", "WO"])
    kind_code = random.choice(["A1", "A2", "B1", "B2", "C1"])

    sample_stucture = {
        "splitId": random.randint(0, 100),
        "start": start,
        "end": end,
        "id": str(uuid.uuid4()),
        "name": f"{country}{pat_num}{kind_code}",
        "number": pat_num,
        "applicationNumber": f"{country}{pub_id}",
        "modifiedOn": get_format("%Y-%m-%d %H:%M:%S"),
        "pubId": f"{country}{pub_id}{kind_code}",
        "systemFamilyId": random.randint(10000000, 90000000),
        "country": country,
        "kindcode": kind_code,
        "publicationDate": get_format("%Y%m%d"),
        "applicationDate": get_format("%Y%m%d"),
        "SearchField": generate_patent_text(),
        "IPCs": [
            {"Sequence": 1, "Value": get_ipc_cpc()},
            {"Sequence": 2, "Value": get_ipc_cpc()},
        ],
        "CPCs": [
            {"Sequence": 1, "Value": get_ipc_cpc()},
            {"Sequence": 2, "Value": get_ipc_cpc()},
            {"Sequence": 3, "Value": get_ipc_cpc()},
            {"Sequence": 4, "Value": get_ipc_cpc()},
            {"Sequence": 5, "Value": get_ipc_cpc()},
        ],
        "source": "feature_combinations",
    }
    return sample_stucture


def generate_test_files(output_dir: str, number_of_files: int) -> None:
    for i in range(number_of_files):
        logger.debug(f"Generating test file {i}")
        doc = yield_sample()
        with open(os.path.join(output_dir, f"{doc.get('id')}.json"), "w") as fh:
            json.dump(doc, fh)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test Data Generator")

    parser.add_argument(
        "-o",
        "--output-folder",
        required=True,
        help="Path to folder with raw ",
        type=str,
    )

    parser.add_argument(
        "-n",
        "--number",
        required=True,
        help="Number of files to generate",
        type=str,
    )

    args = parser.parse_args()

    generate_test_files(args.output_folder, int(args.number))
