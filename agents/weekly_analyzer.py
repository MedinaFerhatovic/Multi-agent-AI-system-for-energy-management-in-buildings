# agents/weekly_analyzer.py
from __future__ import annotations

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from workflow.state_schema import GraphState
from utils.db_helper import connect, insert_anomalies


def _calc_weekly_stats(unit_id: str, conn) -> Optional[Dict[str, Any]]:
    """
    Izračunaj 7-dnevnu statistiku potrošnje za jedinicu
    """
    query = """
    SELECT 
        DATE(timestamp) as day,
        SUM(value) as daily_total,
        AVG(value) as daily_avg
    FROM sensor_readings
    WHERE unit_id = ?
    AND sensor_type = 'energy'
    AND quality_flag = 'ok'
    AND timestamp >= datetime('now', '-7 days')
    GROUP BY DATE(timestamp)
    ORDER BY day
    """
    
    rows = conn.execute(query, (unit_id,)).fetchall()
    
    if not rows:
        return None
    
    daily_totals = [float(row[1]) for row in rows]
    
    return {
        "days_analyzed": len(rows),
        "avg_daily_kwh": round(sum(daily_totals) / len(daily_totals), 2),
        "max_daily_kwh": round(max(daily_totals), 2),
        "min_daily_kwh": round(min(daily_totals), 2),
        "total_weekly_kwh": round(sum(daily_totals), 2),
        "daily_values": daily_totals,
    }


def _detect_weekly_anomalies(unit_id: str, stats: Dict[str, Any], tariff: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Detektuj anomalije na nivou sedmice
    """
    anomalies = []
    
    avg = stats["avg_daily_kwh"]
    max_val = stats["max_daily_kwh"]
    min_val = stats["min_daily_kwh"]
    total = stats["total_weekly_kwh"]
    
    # 1) Ekstremna varijacija (max/min > 3.0)
    if min_val > 0 and (max_val / min_val) > 3.0:
        anomalies.append({
            "unit_id": unit_id,
            "type": "weekly_high_variability",
            "value": round(max_val / min_val, 2),
            "severity": "medium",
            "action": "investigate",
            "category": "weekly_report",
            "details": {
                "max_daily": max_val,
                "min_daily": min_val,
                "ratio": round(max_val / min_val, 2),
                "message": "Daily consumption varies by 3x+ within week",
            },
        })
    
    # 2) Prevelika sedmična potrošnja (threshold: 120 kWh/week za stan)
    weekly_threshold = 120.0  # možeš prilagoditi po clusteru
    if total > weekly_threshold:
        price = float(tariff.get("high_price_per_kwh", 0.18))
        excess_cost = (total - weekly_threshold) * price
        
        anomalies.append({
            "unit_id": unit_id,
            "type": "weekly_budget_exceeded",
            "value": total,
            "severity": "low",
            "action": "notify",
            "category": "weekly_report",
            "details": {
                "total_weekly_kwh": total,
                "weekly_threshold_kwh": weekly_threshold,
                "excess_kwh": round(total - weekly_threshold, 2),
                "excess_cost_estimate": round(excess_cost, 2),
                "currency": tariff.get("currency", "BAM"),
                "message": f"Weekly consumption {round(total - weekly_threshold, 2)} kWh over budget",
            },
        })
    
    # 3) Trend rasta (ako svaki dan više nego prethodni)
    daily_vals = stats["daily_values"]
    if len(daily_vals) >= 5:
        rising_days = sum(1 for i in range(len(daily_vals)-1) if daily_vals[i+1] > daily_vals[i])
        
        if rising_days >= 4:  # bar 4 od 6 dana raste
            anomalies.append({
                "unit_id": unit_id,
                "type": "weekly_consumption_rising_trend",
                "value": daily_vals[-1],
                "severity": "low",
                "action": "monitor",
                "category": "weekly_report",
                "details": {
                    "rising_days_count": rising_days,
                    "total_days": len(daily_vals),
                    "first_day_kwh": round(daily_vals[0], 2),
                    "last_day_kwh": round(daily_vals[-1], 2),
                    "percent_increase": round(((daily_vals[-1] / daily_vals[0]) - 1.0) * 100, 1) if daily_vals[0] > 0 else None,
                    "message": "Consumption increasing trend detected over week",
                },
            })
    
    return anomalies


def weekly_analyzer_node(state: GraphState) -> GraphState:
    """
    Sedmični analizator - pokreće se periodično (npr. jednom dnevno)
    
    Analizira:
    - 7-dnevnu statistiku po jedinici
    - Detektuje sedmične anomalije
    - Upišuje u anomalies_log sa category='weekly_report'
    """
    try:
        building_id = state["building_id"]
        timestamp = state["timestamp"]
        
        with connect() as conn:
            # Get all units
            units = conn.execute(
                "SELECT unit_id FROM units WHERE building_id = ?",
                (building_id,)
            ).fetchall()
            
            tariff_query = """
            SELECT low_tariff_start, low_tariff_end, 
                   low_price_per_kwh, high_price_per_kwh, 
                   sunday_all_day_low, currency
            FROM tariff_model
            WHERE building_id = ?
            """
            tariff_row = conn.execute(tariff_query, (building_id,)).fetchone()
            
            tariff = {
                "low_tariff_start": tariff_row[0] if tariff_row else "22:00",
                "low_tariff_end": tariff_row[1] if tariff_row else "06:00",
                "low_price_per_kwh": tariff_row[2] if tariff_row else 0.08,
                "high_price_per_kwh": tariff_row[3] if tariff_row else 0.18,
                "sunday_all_day_low": tariff_row[4] if tariff_row else 1,
                "currency": tariff_row[5] if tariff_row else "BAM",
            }
            
            all_anomalies = []
            
            for (unit_id,) in units:
                stats = _calc_weekly_stats(unit_id, conn)
                
                if not stats:
                    continue
                
                weekly_anomalies = _detect_weekly_anomalies(unit_id, stats, tariff)
                
                for anomaly in weekly_anomalies:
                    anomaly["timestamp"] = timestamp
                    anomaly["building_id"] = building_id
                
                all_anomalies.extend(weekly_anomalies)
            
            # Insert into DB
            insert_anomalies(conn, all_anomalies)
        
        state["weekly_report"] = {
            "analyzed_units": len(units),
            "anomalies_found": len(all_anomalies),
        }
        
        state["execution_log"].append(
            f"WeeklyAnalyzer: building={building_id} units={len(units)} anomalies={len(all_anomalies)}"
        )
        
    except Exception as e:
        state["errors"].append(str(e))
    
    return state