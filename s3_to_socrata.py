#!/usr/bin/env python3
"""
Download finance json files from s3 bucket and publish them to Socrata
example usage: "python s3_to_socrata.py --dataset fdus"
"""
import argparse
import json
import logging
import os

import boto3
import sodapy

import utils

AWS_ACCESS_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_PASS = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET")

SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_USER = os.getenv("SO_USER")
SO_PASS = os.getenv("SO_PASS")

TASK_DATASET = os.getenv("TASK_DATASET")
DEPT_UNITS_DATASET = os.getenv("DEPT_UNITS_DATASET")
FDU_DATASET = os.getenv("FDU_DATASET")


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
    obj_data = response.get("Body").read().decode()
    data = json.loads(obj_data)

    socrata_client.upsert(DEPT_UNITS_DATASET, data)
    logger.info("Sent units data to Socrata")


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
    obj_data = response.get("Body").read().decode()
    data = json.loads(obj_data)

    # Transforms the file to fit the Socrata dataset
    data = transform_tks(data)

    socrata_client.upsert(TASK_DATASET, data)
    logger.info("Sent task data to Socrata")


def upsert_fdus(client, socrata_client):
    """
    Gets the fdus.json file and upserts data to socrata

    Parameters
    ----------
    client : AWS Client object
    socrata_client : Socrata client object

    Returns
    -------
    None.

    """
    response = client.get_object(Bucket=BUCKET_NAME, Key="fdus.json")
    obj_data = response.get("Body").read().decode()
    data = json.loads(obj_data)

    socrata_client.upsert(FDU_DATASET, data)
    logger.info("Sent fdu to Socrata")


def transform_tks(data):
    """
    Transforms the task_orders.json file to be in the same format as the socrata dataset

    Parameters
    ----------
    data : dict
        Json file from S3 bucket

    Returns
    -------
    replaced_data : dict
        Transformed file.

    """
    columns = {
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

    replaced_data = []
    for row in data:
        new_row = {}
        for src_key, dest_key in columns.items():
            new_row[dest_key] = row.get(src_key)
        new_row["DISPLAY_NAME"] = new_row["TASK_ORDER"] + " | " + new_row["NAME"]
        replaced_data.append(new_row)

    return replaced_data


def main(args):
    # Setting up client objects
    aws_s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
    )
    socrata_client = get_socrata_client()

    # Get a list of the files in the S3 Bucket
    file_list = aws_list_files(aws_s3_client)

    # Argument logic for publishing data
    if args.dataset == "task_orders" or args.dataset == "all":
        # Check if the file is in S3
        if "task_orders.json" in file_list:
            get_task_orders(aws_s3_client, socrata_client)
        else:
            logger.info(
                "No task_orders.json file found in S3 Bucket, nothing happened."
            )
    if args.dataset == "dept_units" or args.dataset == "all":
        # Check if the file is in S3
        if "units.json" in file_list:
            get_dept_unit(aws_s3_client, socrata_client)
        else:
            logger.info("No units.json file found in S3 Bucket, nothing happened.")

    if args.dataset == "fdus" or args.dataset == "all":
        # Check if the file is in S3
        if "fdus.json" in file_list:
            upsert_fdus(aws_s3_client, socrata_client)
        else:
            logger.info("No fdus.json file found in S3 Bucket, nothing happened.")
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dataset",
        type=str,
        choices=["task_orders", "dept_units", "fdus", "all"],
        help=f"Which dataset to publish, defaults to all",
        default="all",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help=f"Sets logger to DEBUG level",
    )

    args = parser.parse_args()

    logger = utils.get_logger(
        __name__, level=logging.DEBUG if args.verbose else logging.INFO,
    )

    main(args)
