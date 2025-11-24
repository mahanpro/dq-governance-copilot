from dataclasses import dataclass

@dataclass
class DQConfig:
    # Later you can add Databricks workspace URLs, catalog/schema names, etc.
    catalog: str = "dq_demo"
    schema: str = "core"