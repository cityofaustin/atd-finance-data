# docker run -it --rm --env-file env_file -v /Users/john/Dropbox/atd/atd-finance-data:/app atddocker/atd-finance-data:production /bin/bash
# python s3_to_knack.py task_orders data-tracker
""" Download financial data from AWS S3 and upsert to a Knack app"""
import argparse
import json
import logging
import os
import sys

import boto3
import knackpy

from config import FIELD_MAPS

BUCKET = os.getenv("BUCKET")
KNACK_APP_ID = os.getenv("KNACK_APP_ID")
KNACK_API_KEY = os.getenv("KNACK_API_KEY")


def cli_args():
    parser = argparse.ArgumentParser(
        description="Extract finance data and load to AWS S3"
    )
    parser.add_argument(
        "name",
        type=str,
        choices=list(FIELD_MAPS.keys()),
        help="The name of the financial data to be processed.",
    )
    parser.add_argument(
        "dest",
        type=str,
        choices=["data-tracker"],
        help="The name of the destination Knack app",
    )
    return parser.parse_args()


def download_json(*, bucket_name, fname):
    """Download a JSON file from S3 and de-serialize it

    Args:
        bucket_name (str): The host bucket name
        fname (str): The file to be downloaded
        as_dict (bool, optional): If the file should be returned a dict. If true,
            assumes file contents are json. Defaults to True.

    Returns:
        list or dict: The decoded and deserialized JSON content
    """
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket_name, fname)
    obj_data = obj.get()["Body"].read().decode()
    return json.loads(obj_data)


def get_pks(fields, app_name):
    """ return the src and destination field name of the primay key """
    pk_field = [f for f in fields if f.get("primary_key")]
    try:
        assert len(pk_field) == 1
    except AssertionError:
        raise ValueError(
            "Multiple primary keys found. There's an error in the field map configuration."
        )
    return pk_field[0]["src"], pk_field[0][app_name]


def is_equal(rec_current, rec_knack):
    tests = [rec_current[key] == rec_knack[key] for key in rec_current.keys()]
    return all(tests)


def create_mapped_record(rec_current, field_map, app_name):
    """Map the data from the current record (from the financial DB) to the destination
    app schema """
    mapped_record = {}

    for field in field_map:
        val = rec_current[field["src"]]
        handler_func = field.get("handler")
        mapped_record[field[app_name]] = val if not handler_func else handler_func(val)

    return mapped_record


def handle_records(records_current, records_knack, field_map, app_name):
    """Compare each current record (from the financial DB) to the data in the
    destination Knack app. If any values have are different, or if the record doesn't
    exist in the destination app, prepare a record payload

    Args:
        records_current (list): The current records from the financial DB
        records_knack (list): The existing records in the desination knack app
        field_map (list): A list of field mapping data (from config.py)
        app_name (str): The name of the destination app.

    Returns:
        list: A list of records to be created or updated in the destination app.
    """
    # identify the primary key field name in the src data and the destination object
    current_pk, knack_pk = get_pks(field_map, app_name)
    todos = []
    for rec_current in records_current:
        matched = False
        id_ = rec_current[current_pk]
        for rec_knack in records_knack:
            if rec_knack[knack_pk] == id_:
                matched = True
                # we create the record payload (and there by apply field mappings and
                # handlers) before we determine if this record needs to be
                # created/modified, this way we make sure we use apples <> apples
                # when comparing the old vs new record
                mapped_record = create_mapped_record(rec_current, field_map, app_name)

                if not is_equal(mapped_record, rec_knack):
                    mapped_record["id"] = rec_knack["id"]
                    todos.append(mapped_record)

                break
        if not matched:
            mapped_record = create_mapped_record(rec_current, field_map, app_name)
            todos.append(mapped_record)

    return todos


def to_csv(data):
    import csv

    with open("bob.csv", "w") as fout:
        writer = csv.DictWriter(fout, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


def main():
    args = cli_args()
    record_type = args.name
    app_name = args.dest
    # get latest finance records from AWS S3
    records_current = download_json(bucket_name=BUCKET, fname=f"{record_type}.json")
    to_csv(records_current)
    # fetch the same type of records from knack
    app = knackpy.App(app_id=KNACK_APP_ID, api_key=KNACK_API_KEY)
    knack_obj = FIELD_MAPS[record_type]["knack_object"][app_name]
    records_knack = [dict(record) for record in app.get(knack_obj)]
    # identify new/changed records and map to destination Knack app schema
    todos = handle_records(
        records_current, records_knack, FIELD_MAPS[record_type]["field_map"], app_name
    )
    breakpoint()
    logging.info(f"{len(todos)} records to process.")

    for record in todos:
        method = "create" if not record.get("id") else "update"
        app.record(data=record, method=method, obj=knack_obj)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    main()
