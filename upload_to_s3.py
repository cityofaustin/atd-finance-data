#!/usr/bin/env python3
# docker run -it --rm --env-file env_file -v /Users/john/Dropbox/atd/atd-finance-data:/app  atddocker/atd-finance-data:production /bin/bash
"""
Fetch financial records from the controller's office DB and **replace** data in AWS S3.

For each record type (e.g., task orders), a single JSON file is uploaded/replaced in S3.
"""
import argparse
import io
import json
import logging
import os
import sys

import boto3
import cx_Oracle

from queries import QUERIES

USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
SERVICE = os.getenv("SERVICE")
BUCKET = os.getenv("BUCKET")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def fileobj(list_of_dicts):
    """ convert a list of dictionaries to a json file-like object """
    return io.BytesIO(json.dumps(list_of_dicts).encode())


def get_conn(host, port, service, user, password):
    # Need to run this once if you want to work locally
    # Change lib_dir to your cx_Oracle library location
    # https://stackoverflow.com/questions/56119490/cx-oracle-error-dpi-1047-cannot-locate-a-64-bit-oracle-client-library
    # lib_dir = r"/Users/charliehenry/instantclient_19_8"
    # cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    dsn_tns = cx_Oracle.makedsn(host, port, service_name=service)
    return cx_Oracle.connect(user=user, password=password, dsn=dsn_tns)


def cli_args():
    parser = argparse.ArgumentParser(
        description="Extract finance data and load to AWS S3"
    )
    parser.add_argument(
        "name",
        type=str,
        choices=list(QUERIES.keys()),
        help="The name of the financial data to be processed.",
    )
    return parser.parse_args()


def main():
    args = cli_args()
    name = args.name
    conn = get_conn(HOST, PORT, SERVICE, USER, PASSWORD)
    query = QUERIES[name]
    cursor = conn.cursor()
    # some queries may take a while to complete:
    # - task orders: ~4 min
    # - units: ~1 min
    # - objects: 30 seconds
    # - master_agreements: 15 seconds
    cursor.execute(query)

    # define row handler which returns each row as a dict
    # h/t https://stackoverflow.com/questions/35045879/cx-oracle-how-can-i-receive-each-row-as-a-dictionary
    cursor.rowfactory = lambda *args: dict(
        zip([d[0] for d in cursor.description], args)
    )
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        raise IOError(
            "No data was retrieved from the financial database. This should never happen!"
        )

    file = fileobj(rows)
    file_name = f"{name}.json"
    session = boto3.session.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    client = session.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    client.upload_fileobj(
        file, BUCKET, file_name,
    )
    logging.info(f"{len(rows)} records processed.")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    main()
