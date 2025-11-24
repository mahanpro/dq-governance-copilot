"""
LangGraph DAG definition for the governance agent.

Router -> fetch_context -> analysis -> sql_safety_judge -> answer.
"""

def build_agent_graph():
    # we will implement this once tools and state are more concrete
    raise NotImplementedError
