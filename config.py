import arrow


""" Handlers must accept and return single value """


def pad_angle_brackets(value):
    """ As it turns out, knack will remove (or at least hide or ignore?) text from a
    text field if it contains an unclosed left angle bracket (<) immediately followed 
    by a alphanum character. Not clear how right angle brackes are hanled, so let's
    just apply spacing to either of them.

    This issue occurs when a task order name contains something like:
    "Blah Blah project on lots <50 acres". It happens. meh. 
    """
    return value.replace(">", " > ").replace("<", " < ")


def is_dept_2400(record):
    return record["DEPT"] == "2400"


def add_comma_separator(value):
    """You may be surprised to learn that knack stores raw "curreny" field values as
    strings with commas/thousands-separators. The "$" is omitted."""
    return f"{value:,.2f}"


def knack_current_timestamp(val, tz="US/Central"):
    """Input val is ignored here as we generate a new value. Note that Knack needs an 
    ISO datestring in local time without the timezone offset, or a "local" timestamp"""
    return arrow.now(tz).format("YYYY-MM-DDTHH:mm:ss")


"""
Each top level key must be a financial record type. you probably dont want to mess w/
these. To add additional destination apps, follow the pattern used with
"data-tracker". Note that where "data-tracker" is used here corresponds to the use
of "app_name" name throughout these scripts as well as in atd-knack-services
"""
FIELD_MAPS = {
    "task_orders": {
        # any Knack app to be processed must have an object ID defined here
        "knack_object": {"data-tracker": "object_86",},
        # see docstring about coalesce in s3_to_knack.py
        "coalesce_fields": ["BYR_FDU"],
        "field_map": [
            {"src": "TK_DEPT", "data-tracker": "field_1276"},
            {"src": "TASK_ORD_CD", "data-tracker": "field_1277", "primary_key": True},
            {
                "src": "TASKORDER_DESC",
                "data-tracker": "field_2632",
                "handler": pad_angle_brackets,
            },
            {"src": "TK_STATUS", "data-tracker": "field_3810",},
            {"src": "TK_TYPE", "data-tracker": "field_3580"},
            {
                "src": "CURRENT_ESTIMATE",
                "data-tracker": "field_3684",
                "handler": add_comma_separator,
            },
            {
                "src": "CHARGEDAMOUNT",
                "data-tracker": "field_3685",
                "handler": add_comma_separator,
            },
            {
                "src": "BALANCE",
                "data-tracker": "field_3686",
                "handler": add_comma_separator,
            },
            {"src": "BYR_FDU", "data-tracker": "field_3807"},
            {
                # appends modified date
                "src": None,
                "data-tracker": "field_3809",
                "handler": knack_current_timestamp,
                "ignore_diff": True,
            },
        ],
    },
    "units": {
        "src_data_filter": {"finance-purchasing": is_dept_2400},
        "knack_object": {
            "data-tracker": "object_189",
            "finance-purchasing": "object_7",
        },
        "field_map": [
            {
                "src": "DEPT_UNIT_ID",
                "data-tracker": "field_3687",
                "primary_key": True,
                "finance-purchasing": "field_902",
            },
            {
                "src": "DEPT_ID",
                "data-tracker": "field_3688",
                "finance-purchasing": "field_903",
            },
            {
                "src": "DEPT",
                "data-tracker": "field_3584",
                "finance-purchasing": "field_431",
            },
            {
                "src": "UNIT",
                "data-tracker": "field_3585",
                "finance-purchasing": "field_79",
            },
            {
                "src": "UNIT_LONG_NAME",
                "data-tracker": "field_3689",
                "finance-purchasing": "field_904",
            },
            {
                "src": "UNIT_SHORT_NAME",
                "data-tracker": "field_3586",
                "finance-purchasing": "field_77",
            },
            {
                "src": "DEPT_UNIT_STATUS",
                "data-tracker": "field_3588",
                "finance-purchasing": "field_488",
            },
        ],
    },
}
