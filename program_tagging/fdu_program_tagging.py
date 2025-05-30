import os
import json
import logging

import boto3

import utils
from prefix_maps import prefix_mapping

AWS_ACCESS_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_PASS = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET")


def get_data(client):
    """
    Gets the subprojects.json file and sends the data to socrata

    Parameters
    ----------
    client : AWS Client object

    socrata_client : Socrata client object

    Returns
    -------
    None.

    """
    response = client.get_object(Bucket=BUCKET_NAME, Key="fdu_expenses_obligated.json")
    obj_data = response.get("Body").read().decode()
    data = json.loads(obj_data)
    return data


def tag_programs(data):
    for row in data:
        row["id"] = (
            row["DEPT"] + row["FUND"] + row["LVL1_DIV_CODE"] + row["LVL2_GP_CODE"]
        )
        for prefix in prefix_mapping:
            if row["id"].startswith(prefix):
                row["program"] = prefix_mapping[prefix]["program"]
                row["subprogram"] = prefix_mapping[prefix]["subprogram"]
                row["bond_year"] = prefix_mapping[prefix]["bond_year"]
    return data


def main():
    aws_s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_ID,
        aws_secret_access_key=AWS_PASS,
    )
    data = get_data(aws_s3_client)
    data = tag_programs(data)
    data


if __name__ == "__main__":
    logger = utils.get_logger(__name__, level=logging.INFO)

    main()
