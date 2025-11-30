"""
SQL safety and LLM-based judges.

The sqlglot-based is_safe_readonly_sql() lives here.
"""

import sqlglot
from sqlglot.expressions import Select

def is_safe_readonly_sql(sql_query: str) -> bool:
    try:
        expr = sqlglot.parse_one(sql_query, read="spark")
    except Exception:
        return False

    # only allow pure SELECT trees, no INSERT/UPDATE/DELETE/CREATE, etc.
    if not isinstance(expr, Select):
        return False

    # optionally, walk the tree and reject if any disallowed nodes appear
    disallowed = {"Insert", "Update", "Delete", "Create", "Drop", "Alter"}
    for node in expr.walk():
        if node.__class__.__name__ in disallowed:
            return False

    return True