"""
SQL safety and LLM-based judges.

The sqlglot-based is_safe_readonly_sql() lives here.
"""
def is_safe_readonly_sql(sql_query: str) -> bool:
    # TODO: implement with sqlglot AST parsing
    # For now, placeholder always returns False to avoid accidental execution.
    return False
