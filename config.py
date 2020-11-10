def pad_angle_brackets(value):
    """ As it turns out, knack will remove (or at least hide or ignore?) text from a
    text field if it contains an unclosed left angle bracket (<) immediately followed 
    by a alphanum character. Not clear how right angle brackes are hanled, so let's
    just apply spacing to either of them.

    This issue occurs when a task order name contains something like:
    "Blah Blah project on lots <50 acres". It happens. meh. 
    """
    return value.replace(">", " > ").replace("<", " < ")


def status_to_bool(value):
    if value.lower() == "active":
        return True
    if value.lower() == "inactive":
        return False
    else:
        raise ValueError(f"Unknown status provided to handler: {value}")


FIELD_MAPS = {
    "task_orders": {
        "knack_object": {"data-tracker": "object_86",},
        "field_map": [
            {"src": "TK_DEPT", "data-tracker": "field_1276"},
            {"src": "TASK_ORD_CD", "data-tracker": "field_1277", "primary_key": True},
            {
                "src": "TASKORDER_DESC",
                "data-tracker": "field_2632",
                "handler": pad_angle_brackets,
            },
            {
                "src": "TK_STATUS",
                "data-tracker": "field_3691",
            },
            {"src": "TK_TYPE", "data-tracker": "field_3580"},
            {"src": "CURRENT_ESTIMATE", "data-tracker": "field_3684"},
            {"src": "CHARGEDAMOUNT", "data-tracker": "field_3685"},
            {"src": "BALANCE", "data-tracker": "field_3686"},
        ],
    },
    "units": {
        "knack_object": {"data-tracker": "object_189",},
        "field_map": [
            {"src": "DEPT_UNIT_ID", "data-tracker": "field_3687", "primary_key": True},
            {"src": "DEPT_ID", "data-tracker": "field_3688"},
            {"src": "DEPT", "data-tracker": "field_3584"},
            {"src": "UNIT", "data-tracker": "field_3585"},
            {"src": "UNIT_LONG_NAME", "data-tracker": "field_3689"},
            {"src": "UNIT_SHORT_NAME", "data-tracker": "field_3586"},
            {
                "src": "DEPT_UNIT_STATUS",
                "data-tracker": "field_3588",
            },
        ],
    },
}
