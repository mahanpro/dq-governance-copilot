"""
LangGraph agent state definition.
"""
from typing import List, Optional, TypedDict

class AgentState(TypedDict, total=False):
    question: str
    focus_table: Optional[str]
    focus_column: Optional[str]
    # we will define Incident, ProfileSummary, LineageEdge types later
    incidents: List[dict]
    profiles: List[dict]
    lineage: List[dict]
    suggestions: List[str]
    judge_scores: List[float]
    sql_safety_flags: List[bool]
    draft_answer: Optional[str]
    iterations: int
