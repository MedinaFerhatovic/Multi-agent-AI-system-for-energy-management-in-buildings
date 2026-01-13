import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from workflow.langgraph_workflow import build_graph, make_initial_state
from scripts import feature_extractor, clustering

DB_PATH = BASE_DIR / "db" / "smartbuilding.db"


if __name__ == "__main__":
    feature_extractor.run(str(DB_PATH), "B001")
    clustering.run(str(DB_PATH), "B001", n_clusters=None)

    graph = build_graph()

    initial_state = make_initial_state("B001")
    result = graph.invoke(initial_state)

    print("LOG:", result["execution_log"])
    print("ERRORS:", result["errors"])
    print("FINAL_DECISIONS:", result["final_decisions"])
