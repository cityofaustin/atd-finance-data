# atd-finance-data

These are Python utilities for extracting and publishing financial data from the City of Austin Controller's Office database.

The utilities deal with four types of financial records:

- Task Orders: funding codes which are used to associate labor costs with a specific task or project budget.
- Unit codes: funding codes which tie spending to a sub-departmental workgroup's (e.g., a Division) budget.
- Object codes: funding codes which identify spending categories, e.g. "Misc. office supplies".
- Master Agreements: contractual agreements between the City and its vendors.

At a high level, these utilities do two things:
1. Extract these financial records from the Controller's Office Oracle database and upload them as JSON files to an AWS S3 bucket.

2. Download these records from AWS S3 and publish them to ATD's back-office systems, such as the AMD Data Tracker (for warehouse inventory) and the Finance & Purchasing system (for purchase requests).


