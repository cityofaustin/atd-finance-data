# atd-finance-data

These are Python utilities for extracting and publishing financial data from the City of Austin Controller's Office Microstrategy database.

The utilities deal with four types of financial records:

- Task Orders (`task_orders`): funding codes which are used to associate labor costs with a specific task or project budget.
- Unit codes (`units`): funding codes which tie spending to a sub-departmental workgroup's (e.g., a Division) budget.
- Object codes (`objects`): funding codes which identify spending categories, e.g. "Misc. office supplies".
- Master Agreements (`master_agreements`): contractual agreements between the City and its vendors.

At a high level, these utilities do two things:

1. Extract these financial records from the Controller's Office Microstrategy (Oracle) database and upload them as JSON files (one file per record type) to an AWS S3 bucket.

2. Download these records from AWS S3 and publish them to ATD's back-office systems, such as the AMD Data Tracker (for warehouse inventory) and the Finance & Purchasing system (for purchase requests).

## Setup

You should know that scripts from this repo run on [ATD's Airflow instance](https://github.com/cityofaustin/atd-airflow).

The easiest path to running these utilities locally is to build this repo's Docker image, or pull it from `atddocker/atd-finance-data`. This container builds on top of [atd-oracle-py](https://github.com/cityofaustin/atd-oracle-py), which solves the headache of installing `cx_Oracle`.

### Docker CI

Any push to the `production` or `staging` branches will trigger a docker build/push to Docker hub with all three of the `production`, `staging`, and `latest` tags, as is required to ensure that Docker images are kept in sync with our Airflow isntance.

### Configuration (`config.py`)

Publishing records to a Knack app (`s3_to_knack.py`) relies on `config.py`. It contains a `FIELD_MAPS` dictionary which defines all field name mappings and translation functions (aka, handlers).

Each top-level key in the `FIELD_MAPS` dict represents a supported record type to be processed (`task_orders`, `units`, `master_agreements`, `objects`), and contains a list of fields with their mapping definition. The mapping definitions should be structured like so:

- `src` (`str`, required): The name of the field in the source data from Microstrategy
- `<app-name>` (`str`, required): The name of the field in the desintation <app-name>. See the `config.py` docstrings and [atd-knack-services](https://github.com/cityofaustin/atd-knack-services) for more details on app names.
- `primary_key` (`bool`, optional): If the field is the dataset's primary key.
- `handler` (`function`, optional): An optional translation function to be applied to the `src` data when mapping to the destination field.
- `ignore_diff` (`bool`, optional): If `True`, this field will not be evaluated when comparing the difference between Microstrategy data and Knack data.

## Uploading records to AWS S3

`upload_to_s3.py` queries the financial database for a given record type and uploads the results to a single JSON file in S3. Because all records are stored in a single JSON file, the data is completely replaced on each run. The record type must be specified as a positional CLI argument, like so:

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

## Upsert records to a Knack app

`s3_to_knack.py` downloads financial records from S3 and upserts them into a Knack app. Data is processed incrementally by comparing the difference between the source data from Microstrategy to the data in the destination Knack app. To accomplish this, all records are fetched from both S3 and the destination Knack app on each job run. As such, **this task should be scheduled during off-peak hours** to offest the API load on the destination Knack app.

The record type and destination app must be specified as positional CLI arguments, like so:

```shell
$ python s3_to_knack.py task_orders data-tracker
```

Any destination app must have a field mapping defined in `config.py`.

Required environmental variables, which are available in the DTS credential store:

- `BUCKET`: The destination S3 bucket name on AWS
- `AWS_ACCESS_KEY_ID`: The access key for your AWS account
- `AWS_SECRET_ACCESS_KEY`: The secret key for your AWS account
- `KNACK_APP_ID`: The Knack app ID of the destiantion knack app
- `KNACK_API_KEY`: The kanck API key of the destination knack app
