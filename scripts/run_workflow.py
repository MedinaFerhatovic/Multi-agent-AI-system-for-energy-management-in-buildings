#!/usr/bin/env python3
# scripts/run_workflow.py - ENHANCED RUNNER

import sqlite3
import sys
import traceback
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from workflow.graph import create_graph
from workflow.state_schema import GraphState
from utils.db_helper import connect

DB_PATH = BASE_DIR / "db" / "smartbuilding.db"


def get_max_timestamp_from_db(building_id: str) -> str:
    """Get latest sensor reading timestamp for deterministic offline run"""
    with connect() as conn:
        cursor = conn.execute(
            """
            SELECT MAX(timestamp) 
            FROM sensor_readings 
            WHERE building_id = ? AND quality_flag = 'ok'
            """,
            (building_id,)
        )
        row = cursor.fetchone()
        if not row or not row[0]:
            raise ValueError(f"No sensor_readings found for building {building_id}")
        return row[0]


def run_workflow_for_building(building_id: str, policy: dict = None) -> dict:
    """
    Run complete workflow for one building.
    
    Returns:
        dict with keys: state, success, errors, execution_time
    """
    start_time = datetime.now()
    
    try:
        # Get anchor timestamp (deterministic)
        anchor_ts = get_max_timestamp_from_db(building_id)
        
        print(f"\n{'='*70}")
        print(f"🚀 WORKFLOW START: {building_id}")
        print(f"{'='*70}")
        print(f"⏰ Anchor timestamp: {anchor_ts}")
        print(f"📋 Policy: {policy or 'default'}")
        print(f"{'='*70}\n")
        
        # Initialize state
        initial_state = GraphState(
            building_id=building_id,
            timestamp=anchor_ts,
            policy=policy or {},
            sensor_data={},
            validated_data={},
            anomalies=[],
            predictions={},
            optimization_plans={},
            final_decisions=[],
            validation_report={},
            errors=[],
            execution_log=[],
        )
        
        # Create and run graph
        graph = create_graph()
        final_state = graph.invoke(initial_state)
        
        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n{'='*70}")
        print(f"✅ WORKFLOW COMPLETE: {building_id}")
        print(f"{'='*70}")
        print(f"⏱️  Duration: {duration:.2f}s")
        print(f"📊 Stats:")
        print(f"   - Units monitored: {len(final_state.get('sensor_data', {}))}")
        print(f"   - Predictions: {len(final_state.get('predictions', {}))}")
        print(f"   - Plans: {len(final_state.get('optimization_plans', {}))}")
        print(f"   - Decisions: {len(final_state.get('final_decisions', []))}")
        print(f"   - Anomalies: {len(final_state.get('anomalies', []))}")
        print(f"   - Errors: {len(final_state.get('errors', []))}")
        
        if final_state.get('errors'):
            print(f"\n⚠️  ERRORS:")
            for err in final_state['errors']:
                print(f"   - {err}")
        
        print(f"\n📝 Execution Log:")
        for log in final_state.get('execution_log', []):
            print(f"   {log}")
        
        validation = final_state.get('validation_report', {})
        if validation:
            print(f"\n🔍 Validation Report:")
            print(f"   - Status: {validation.get('status', 'unknown')}")
            print(f"   - Confidence: {validation.get('model_confidence_avg', 0):.2f}")
            print(f"   - Coverage: {validation.get('coverage', 0):.2f}")
            print(f"   - Blocked units: {len(validation.get('block_units', []))}")
            if validation.get('reasons'):
                print(f"   - Reasons: {', '.join(validation['reasons'])}")
        
        print(f"{'='*70}\n")
        
        return {
            "state": final_state,
            "success": len(final_state.get('errors', [])) == 0,
            "errors": final_state.get('errors', []),
            "execution_time": duration,
        }
    
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n{'='*70}")
        print(f"❌ WORKFLOW FAILED: {building_id}")
        print(f"{'='*70}")
        print(f"⏱️  Duration: {duration:.2f}s")
        print(f"💥 Error: {str(e)}")
        print(f"\n🔍 Traceback:")
        traceback.print_exc()
        print(f"{'='*70}\n")
        
        return {
            "state": None,
            "success": False,
            "errors": [str(e)],
            "execution_time": duration,
        }


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run multi-agent workflow")
    parser.add_argument("--building", required=True, help="Building ID (e.g., B001)")
    parser.add_argument("--policy", help="Policy name (default, aggressive, conservative)")
    parser.add_argument("--db", default=str(DB_PATH), help="Database path")
    
    args = parser.parse_args()
    
    # Policy presets
    policies = {
        "default": {"cost_weight": 1.0, "comfort_weight": 0.8, "stability_weight": 0.4},
        "aggressive": {"cost_weight": 1.5, "comfort_weight": 0.5, "stability_weight": 0.2},
        "conservative": {"cost_weight": 0.7, "comfort_weight": 1.2, "stability_weight": 0.8},
    }
    
    policy = policies.get(args.policy, policies["default"])
    
    # Check DB exists
    if not Path(args.db).exists():
        print(f"❌ Database not found: {args.db}")
        sys.exit(1)
    
    # Run workflow
    result = run_workflow_for_building(args.building, policy)
    
    # Exit code
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()