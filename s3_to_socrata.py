#!/usr/bin/env python3
"""
Download finance json files and publish them to Socrata
"""
import argparse
from datetime import datetime, timezone, timedelta
import json
import logging
import os
import numpy as np

import boto3
import sodapy
import pandas as pd

import utils

AWS_ACCESS_ID = os.getenv("AWS_ACCESS_ID")
AWS_PASS = os.getenv("AWS_PASS")
BUCKET_NAME = os.getenv("BUCKET_NAME")

SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_USER = os.getenv("SO_USER")
SO_PASS = os.getenv("SO_PASS")

TASK_DATASET = os.getenv("TASK_DATASET")
DEPT_UNITS_DATASET = os.getenv("DEPT_UNITS_DATASET")


def get_socrata_client():
    return sodapy.Socrata(
        SO_WEB, SO_TOKEN, username=SO_USER, password=SO_PASS, timeout=60,
    )


def aws_list_files(client):
    """
    Returns a list of files in an S3 bucket.
    :return: object
    """
    response = client.list_objects(Bucket=BUCKET_NAME,)

    file_list = []
    for content in response.get("Contents", []):
        file_list.append(content.get("Key"))

    return file_list


def get_dept_unit(client, socrata_client):
    """
    Gets the units.json file and sends the data to socrata 

    Parameters
    ----------
    client : AWS Client object
    
    socrata_client : Socrata client object

    Returns
    -------
    None.

    """
    response = client.get_object(Bucket=BUCKET_NAME, Key="units.json")
    data = json.load(response.get("Body"))
    socrata_client.upsert(DEPT_UNITS_DATASET, data)
    logger.debug("Sent units data to Socrata")


def get_task_orders(client, socrata_client):
    """
    Gets the task_orders.json file and sends the data to socrata 

    Parameters
    ----------
    client : AWS Client object
        
    socrata_client : Socrata client object

    Returns
    -------
    None.

    """
    response = client.get_object(Bucket=BUCKET_NAME, Key="task_orders.json")

    df = pd.read_json(response.get("Body"), orient="records")
    # Transforms the file to fit the Socrata dataset
    data = transform_records(df)

    socrata_client.upsert(TASK_DATASET, data)
    logger.debug("Sent task data to Socrata")


def transform_records(df):
    """
    Transforms the task_orders.json file to be in the same format as the socrata dataset

    Parameters
    ----------
    df : Pandas Dataframe
        Raw JSON file from S3.

    Returns
    -------
    Dict
        Transformed file.

    """
    # Replaces NaN float values to None which is needed for Socrata.
    # Socrata doesn't recognize NaN as a value for number columns
    df = df.replace({np.nan: None})

    df = df.rename(
        columns={
            "TASK_ORDER_DEPT": "DEPT",
            "TASK_ORDER_ID": "TASK_ORDER",
            "TASK_ORDER_DESC": "NAME",
            "TASK_ORDER_STATUS": "Status",
            "TASK_ORDER_TYPE": "TK_TYPE",
            "TK_CURR_AMOUNT": "CURRENT_ESTIMATE",
            "CHARGED_AMOUNT": "CHARGEDAMOUNT",
            "TASK_ORDER_BAL": "BALANCE",
            "BYR_FDU": "BUYER_FDUS",
        }
    )

    # Creating Display Name field
    df["DISPLAY_NAME"] = df["TASK_ORDER"] + " | " + df["NAME"]

    return df.to_dict("records")


def main(args):

    ## Setting up client objects
    aws_s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
    )
    socrata_client = get_socrata_client()

    # Get a list of the files in the the S3 Bucekt
    file_list = aws_list_files(aws_s3_client)

    # Argument logic for publishing data
    if args.dataset == "task_orders" or args.dataset == "both":
        # Check if the file is in S3
        if "task_orders.json" in file_list:
            get_task_orders(aws_s3_client, socrata_client)
        else:
            logger.debug(
                "No task_orders.json file found in S3 Bucket, nothing happened."
            )
    if args.dataset == "dept_units" or args.dataset == "both":
        # Check if the file is in S3
        if "units.json" in file_list:
            get_dept_unit(aws_s3_client, socrata_client)
        else:
            logger.debug("No units.json file found in S3 Bucket, nothing happened.")
    return


parser = argparse.ArgumentParser()

parser.add_argument(
    "--dataset",
    type=str,
    choices=["task_orders", "dept_units", "both"],
    help=f"Which dataset to publish, defaults to both",
    default="both",
)

parser.add_argument(
    "-v", "--verbose", action="store_true", help=f"Sets logger to DEBUG level",
)

args = parser.parse_args()

logger = utils.get_logger(
    __name__, level=logging.DEBUG if args.verbose else logging.INFO,
)

main(args)
