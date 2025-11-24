"""
Tool functions for the agent.

These will call Databricks (Spark SQL / Unity Catalog) to fetch:
- table schema
- dq_profiles
- dq_incidents
- lineage info

For now, they are placeholders so unit tests can be wired later.
"""

def get_table_schema(table_name: str):
    """
    Return column names and types for the given table.

    The agent should call this BEFORE generating any SQL for that table,
    to avoid referencing non-existent columns.
    """
    raise NotImplementedError

