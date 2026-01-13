import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agents.weekly_analyzer import weekly_analyzer_node
from workflow.state_schema import GraphState

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "smartbuilding.db"


def run_weekly_analysis(building_id: str):
    print(f"\n{'='*60}")
    print(f"WEEKLY ENERGY ANALYSIS - {building_id}")
    print(f"{'='*60}\n")
    
    # Pripremimo state
    state: GraphState = {
        "building_id": building_id,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sensor_data": {},
        "validated_data": {},
        "anomalies": [],
        "predictions": {},
        "optimization_plans": {},
        "final_decisions": [],
        "execution_log": [],
        "errors": [],
    }
    
    result_state = weekly_analyzer_node(state)
    
    print("\nRESULTS:")
    print(f"  - Execution log: {len(result_state['execution_log'])} entries")
    
    for log_entry in result_state['execution_log']:
        print(f"    {log_entry}")
    
    if result_state.get('weekly_report'):
        report = result_state['weekly_report']
        print(f"\n  - Analyzed units: {report['analyzed_units']}")
        print(f"  - Anomalies found: {report['anomalies_found']}")
    
    if result_state.get('errors'):
        print(f"\nERRORS:")
        for err in result_state['errors']:
            print(f"  - {err}")
    
    print(f"\n{'='*60}")
    print("Weekly analysis complete!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run weekly energy analysis")
    parser.add_argument(
        "--building", 
        default="B001", 
        help="Building ID (default: B001)"
    )
    
    args = parser.parse_args()
    
    run_weekly_analysis(args.building)