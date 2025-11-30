"""
In-memory asset and lineage graph.

In production this would use Databricks system lineage tables
(e.g. system.lineage.table_lineage and system.lineage.column_lineage).
Here we will simulate lineage with a static mapping.

Nodes: Table, Column, Job, Incident.
Edges: Table->Column, Column->Incident, Job->Table, etc.
"""

from dataclasses import dataclass
from typing import List, Dict

@dataclass
class TableNode:
    name: str
    owner_team: str

@dataclass
class ColumnNode:
    table: str
    name: str

@dataclass
class IncidentNode:
    id: str
    table: str
    column: str