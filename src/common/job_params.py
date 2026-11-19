"""Read a job parameter from a notebook widget or a `--name=value` script argument."""

import sys


def get_param(name: str, default: str = "") -> str:
    try:                                  # notebook task
        # A notebook's injected `dbutils` isn't visible inside an imported
        # module, so take it from the SDK runtime instead.
        from databricks.sdk.runtime import dbutils
        return dbutils.widgets.get(name)
    except Exception:
        pass
    for arg in sys.argv[1:]:              # python file task: --name=value
        if arg.startswith(f"--{name}="):
            return arg.split("=", 1)[1]
    return default
