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

USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
SERVICE = os.getenv("SERVICE")
BUCKET = os.getenv("BUCKET")

# we are explicit about the fields we select not only because these views hold data we
# don't care about but also because any datetime fields would require extra handling in
# order to JSON-serialize them
QUERIES = {
    "task_orders": "select atd_tk.TASK_ORDER_DEPT, atd_tk.TASK_ORDER_ID, atd_tk.TASK_ORDER_DESC, atd_tk.TASK_ORDER_STATUS, atd_tk.TASK_ORDER_TYPE, atd_tk.TK_CURR_AMOUNT, atd_tk.CHARGED_AMOUNT, atd_tk.TASK_ORDER_BAL, buyer_tk.BYR_FDU FROM DEPT_2400_TK_VW atd_tk LEFT JOIN( SELECT DISTINCT TASK_ORD_CD, BYR_FDU FROM REL_BUYER_SELLER_FDU_TK) buyer_tk ON atd_tk.TASK_ORDER_ID = buyer_tk.TASK_ORD_CD where TASK_ORDER_STATUS is not null",
    "units": "select DEPT_UNIT_ID, DEPT_ID, DEPT, UNIT, UNIT_LONG_NAME, UNIT_SHORT_NAME, DEPT_UNIT_STATUS from lu_dept_units WHERE DEPT in(2400,6207,2507)",
    "objects": "select OBJ_ID, OBJ_CLASS_ID, OBJ_CATEGORY_ID, OBJ_TYPE_ID, OBJ_GROUP_ID, OBJ_CODE, OBJ_LONG_NAME, OBJ_SHORT_NAME, OBJ_DESC, OBJ_REIMB_ELIG_STATUS, OBJ_STATUS, ACT_FL from lu_obj_cd",
    "master_agreements": "select DOC_CD, DOC_DEPT_CD, DOC_ID, DOC_DSCR, DOC_PHASE_CD, VEND_CUST_CD, LGL_NM from DEPT_2400_MA_VW",
    "fdus": "select * from ATD_SUBPROJECT_FDU_VW",
}


def fileobj(list_of_dicts):
    """ convert a list of dictionaries to a json file-like object """
    return io.BytesIO(json.dumps(list_of_dicts).encode())


def get_conn(host, port, service, user, password):
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
    session = boto3.session.Session()
    client = session.client("s3")
    client.upload_fileobj(
        file, BUCKET, file_name,
    )
    logging.info(f"{len(rows)} records processed.")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    main()

