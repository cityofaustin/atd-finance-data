# atd-finance-data

These are Python utilities for extracting and publishing financial data from the City of Austin Controller's Office database.

The utilities deal with four types of financial records:

- Task Orders (`task_orders`): funding codes which are used to associate labor costs with a specific task or project budget.
- Unit codes (`units`): funding codes which tie spending to a sub-departmental workgroup's (e.g., a Division) budget.
- Object codes (`objects`): funding codes which identify spending categories, e.g. "Misc. office supplies".
- Master Agreements (`master_agreements`): contractual agreements between the City and its vendors.

At a high level, these utilities do two things:

1. Extract these financial records from the Controller's Office Oracle database and upload them as JSON files (one file per record type) to an AWS S3 bucket.

2. Download these records from AWS S3 and publish them to ATD's back-office systems, such as the AMD Data Tracker (for warehouse inventory) and the Finance & Purchasing system (for purchase requests).

## Setup

You should know that scripts from this repo run on [ATD's Airflow instance](https://github.com/cityofaustin/atd-airflow).

The easiest path to running these utilities locally to build this repo's Docker image, or pull it from `atddocker/atd-finance-data`.
This container builds on top of [atd-oracle-py](https://github.com/cityofaustin/atd-oracle-py), which solves the headache of installing `cx_Oracle`.

## Uploading records to AWS S3

`upload_to_s3.py` queries the financial database for a given record type and uploads the results to a single JSON file in S3. The record type must be specified as a positional CLI argument, like so:

```shell
$ python upload_to_s3.py task_orders
```

Required environmental variables, which are available in the DTS credential store:

- `USER`: The financial DB user name
- `PASSWORD`: The financial DB user's password
- `HOST`: The financial DB's host address
- `PORT`: The financial DB's port
- `SERVICE`: The financial DB's service name
- `BUCKET`: The destination S3 bucket name on AWS
- `AWS_ACCESS_KEY_ID`: The access key for your AWS account
- `AWS_SECRET_ACCESS_KEY`: The secret key for your AWS account
