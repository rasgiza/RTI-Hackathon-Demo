#!/usr/bin/env python3
"""
Deploy Bicycle Fleet Graph Model via Fabric REST API
=====================================================

Creates a standalone GraphModel item in the Bike Rental Hackathon workspace
with a full public definition derived from the Bicycle_Fleet_Ontology metadata.

The definition consists of four parts:
  - graphType.json            Node/edge type schema
  - graphDefinition.json      Data mappings (property → column)
  - dataSources.json          OneLake delta table paths
  - stylingConfiguration.json Layout & visual defaults

Entity types and relationships are hard-coded from BICYCLE_ONTOLOGY_GUIDE.md
(13 entity types, 24 relationships, all bound to bicycles_gold lakehouse).

API reference:
  POST /workspaces/{id}/graphModels              -- create with definition
  POST /workspaces/{id}/graphModels/{id}/updateDefinition
  POST /workspaces/{id}/items/{id}/jobs/instances -- RefreshGraph

Usage:
    python deploy_graph_model.py
    python deploy_graph_model.py --update
    python deploy_graph_model.py --graph-model "My_Graph_Name"
"""

import sys
import json
import base64
import time
import math
import uuid
import argparse
import requests

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from fabric_auth import get_fabric_token, get_auth_headers
from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

# ============================================================
# CONFIGURATION
# ============================================================
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
GRAPH_MODEL_NAME = "Bicycle_Fleet_Graph"
LAKEHOUSE_NAME = "bicycles_gold"

# Workspace & Lakehouse IDs (resolved at runtime, fallback hardcoded)
WORKSPACE_ID_FALLBACK = "573cc7c7-a45a-4fd9-886e-9db4e9798db4"
LAKEHOUSE_ID_FALLBACK = "b05b765b-b136-4a6f-b56f-9ab928fb3e78"

# Ontology valueType → GraphModel property type
TYPE_MAP = {
    "String": "STRING",
    "BigInt": "INT",
    "Int": "INT",
    "Double": "FLOAT",
    "DateTime": "DATETIME",
    "Boolean": "BOOLEAN",
}


# ============================================================
# ENTITY TYPE DEFINITIONS (from BICYCLE_ONTOLOGY_GUIDE.md)
# ============================================================
# Each entity: name, pk_column, bound_table, properties[(col_name, type)]

ENTITY_TYPES = [
    {
        "name": "Station",
        "pk": "station_key",
        "table": "dim_station",
        "properties": [
            ("station_key", "BigInt"),
            ("station_id", "String"),
            ("street_address", "String"),
            ("neighbourhood", "String"),
            ("latitude", "Double"),
            ("longitude", "Double"),
            ("total_docks", "BigInt"),
            ("station_size", "String"),
            ("current_status", "String"),
            ("current_utilization_pct", "Double"),
            ("rebalance_priority", "Double"),
            ("zone", "String"),
        ],
    },
    {
        "name": "Neighbourhood",
        "pk": "neighbourhood_key",
        "table": "dim_neighbourhood",
        "properties": [
            ("neighbourhood_key", "BigInt"),
            ("neighbourhood_name", "String"),
            ("station_count", "BigInt"),
            ("total_capacity", "BigInt"),
            ("total_bikes_available", "BigInt"),
            ("total_empty_docks", "BigInt"),
            ("neighbourhood_utilization_pct", "Double"),
            ("avg_utilization_pct", "Double"),
            ("health_score", "Double"),
            ("capacity_status", "String"),
            ("density_tier", "String"),
        ],
    },
    {
        "name": "TimeSlot",
        "pk": "time_key",
        "table": "dim_time",
        "properties": [
            ("time_key", "BigInt"),
            ("hour_of_day", "BigInt"),
            ("hour_label", "String"),
            ("time_period", "String"),
            ("is_rush_hour", "Boolean"),
            ("am_pm", "String"),
            ("demand_tier", "String"),
        ],
    },
    {
        "name": "CalendarDate",
        "pk": "date_key",
        "table": "dim_date",
        "properties": [
            ("date_key", "BigInt"),
            ("date_value", "DateTime"),
            ("year", "BigInt"),
            ("quarter", "BigInt"),
            ("month_name", "String"),
            ("day_name", "String"),
            ("is_weekend", "Boolean"),
            ("season", "String"),
        ],
    },
    {
        "name": "WeatherObservation",
        "pk": "weather_key",
        "table": "dim_weather",
        "properties": [
            ("weather_key", "BigInt"),
            ("observation_hour", "DateTime"),
            ("weather_description", "String"),
            ("weather_category", "String"),
            ("weather_severity", "String"),
            ("temperature_c", "Double"),
            ("feels_like_c", "Double"),
            ("wind_speed_kmh", "Double"),
            ("has_precipitation", "Boolean"),
            ("cycling_comfort_index", "Double"),
        ],
    },
    {
        "name": "AvailabilityEvent",
        "pk": "availability_key",
        "table": "fact_availability",
        "properties": [
            ("availability_key", "BigInt"),
            ("station_key", "BigInt"),
            ("time_key", "BigInt"),
            ("date_key", "BigInt"),
            ("event_timestamp", "DateTime"),
            ("bikes_available", "Int"),
            ("empty_docks", "Int"),
            ("total_docks", "Int"),
            ("utilization_pct", "Double"),
            ("is_critical", "Boolean"),
            ("availability_band", "String"),
        ],
    },
    {
        "name": "HourlyDemand",
        "pk": "demand_key",
        "table": "fact_hourly_demand",
        "properties": [
            ("demand_key", "BigInt"),
            ("neighbourhood_key", "BigInt"),
            ("time_key", "BigInt"),
            ("date_key", "BigInt"),
            ("event_hour", "DateTime"),
            ("event_count", "BigInt"),
            ("avg_utilization", "Double"),
            ("critical_events", "BigInt"),
            ("rebalance_triggers", "BigInt"),
            ("demand_intensity", "String"),
        ],
    },
    {
        "name": "RebalancingAssessment",
        "pk": "rebalance_key",
        "table": "fact_rebalancing",
        "properties": [
            ("rebalance_key", "BigInt"),
            ("station_key", "BigInt"),
            ("neighbourhood_key", "BigInt"),
            ("time_key", "BigInt"),
            ("weather_key", "BigInt"),
            ("date_key", "BigInt"),
            ("station_id", "String"),
            ("neighbourhood", "String"),
            ("priority_score", "Double"),
            ("recommended_action", "String"),
            ("bikes_to_target", "Int"),
            ("availability_status", "String"),
            ("estimated_rebalance_cost", "Double"),
        ],
    },
    {
        "name": "WeatherImpact",
        "pk": "weather_impact_key",
        "table": "fact_weather_impact",
        "properties": [
            ("weather_impact_key", "BigInt"),
            ("weather_key", "BigInt"),
            ("neighbourhood_key", "BigInt"),
            ("time_key", "BigInt"),
            ("date_key", "BigInt"),
            ("event_hour", "DateTime"),
            ("weather_adjusted_demand", "Double"),
            ("weather_demand_impact", "Double"),
            ("impact_category", "String"),
            ("demand_gap", "Double"),
        ],
    },
    {
        "name": "DemandForecast",
        "pk": "forecast_key",
        "table": "forecast_demand",
        "properties": [
            ("forecast_key", "BigInt"),
            ("forecast_hour", "DateTime"),
            ("neighbourhood", "String"),
            ("neighbourhood_key", "BigInt"),
            ("time_key", "BigInt"),
            ("date_key", "BigInt"),
            ("hour_of_day", "Int"),
            ("day_of_week", "Int"),
            ("is_rush_hour", "Boolean"),
            ("is_weekend", "Boolean"),
            ("predicted_demand", "Double"),
            ("demand_lower_bound", "Double"),
            ("demand_upper_bound", "Double"),
            ("demand_tier", "String"),
            ("pre_position_recommended", "Boolean"),
            ("forecast_generated_at", "DateTime"),
            ("model_quality", "String"),
        ],
    },
    {
        "name": "StationSnapshot",
        "pk": "station_key",
        "table": "gold_station_snapshot",
        "properties": [
            ("station_key", "BigInt"),
            ("station_id", "String"),
            ("street_address", "String"),
            ("neighbourhood", "String"),
            ("latitude", "Double"),
            ("longitude", "Double"),
            ("total_docks", "BigInt"),
            ("station_size", "String"),
            ("zone", "String"),
            ("bikes_available", "Int"),
            ("empty_docks", "Int"),
            ("utilization_pct", "Double"),
            ("availability_band", "String"),
            ("is_critical", "Boolean"),
            ("last_event_at", "DateTime"),
            ("last_event_type", "String"),
            ("rebalance_priority", "Double"),
            ("operational_status", "String"),
            ("needs_attention", "Boolean"),
            ("snapshot_at", "DateTime"),
        ],
    },
    {
        "name": "RecentAvailability",
        "pk": "recent_key",
        "table": "gold_availability_recent",
        "properties": [
            ("recent_key", "BigInt"),
            ("station_key", "BigInt"),
            ("station_id", "String"),
            ("street_address", "String"),
            ("neighbourhood", "String"),
            ("zone", "String"),
            ("event_hour", "DateTime"),
            ("event_date", "DateTime"),
            ("time_key", "BigInt"),
            ("date_key", "BigInt"),
            ("event_count", "BigInt"),
            ("avg_bikes", "Double"),
            ("min_bikes", "Int"),
            ("max_bikes", "Int"),
            ("avg_empty_docks", "Double"),
            ("avg_utilization", "Double"),
            ("critical_events", "BigInt"),
            ("utilization_band", "String"),
        ],
    },
    {
        "name": "Customer",
        "pk": "CustomerID",
        "table": "dim_customers",
        "properties": [
            ("CustomerID", "String"),
            ("FirstName", "String"),
            ("LastName", "String"),
            ("Email", "String"),
            ("PhoneNumber", "String"),
            ("PostalCode", "String"),
            ("Neighbourhood", "String"),
            ("MembershipTier", "String"),
            ("MemberSince", "String"),
            ("RiderSegment", "String"),
            ("MarketingOptIn", "Boolean"),
            ("AccountStatus", "String"),
        ],
    },
]


# ============================================================
# RELATIONSHIP DEFINITIONS (from BICYCLE_ONTOLOGY_GUIDE.md)
# ============================================================
# Each relationship: name, source_entity, target_entity, join_column
# The join_column exists in the source entity's table.

RELATIONSHIPS = [
    # AvailabilityEvent edges
    ("located_at", "AvailabilityEvent", "Station", "station_key"),
    ("occurred_during", "AvailabilityEvent", "TimeSlot", "time_key"),
    ("event_on_date", "AvailabilityEvent", "CalendarDate", "date_key"),
    # HourlyDemand edges
    ("measured_in", "HourlyDemand", "Neighbourhood", "neighbourhood_key"),
    ("demand_at_time", "HourlyDemand", "TimeSlot", "time_key"),
    ("demand_on_date", "HourlyDemand", "CalendarDate", "date_key"),
    # RebalancingAssessment edges
    ("assessed_for", "RebalancingAssessment", "Station", "station_key"),
    ("rebalance_in_zone", "RebalancingAssessment", "Neighbourhood", "neighbourhood_key"),
    ("rebalance_at_time", "RebalancingAssessment", "TimeSlot", "time_key"),
    ("assessed_on_date", "RebalancingAssessment", "CalendarDate", "date_key"),
    ("rebalance_during_weather", "RebalancingAssessment", "WeatherObservation", "weather_key"),
    # WeatherImpact edges
    ("affected_by", "WeatherImpact", "WeatherObservation", "weather_key"),
    ("impacts_zone", "WeatherImpact", "Neighbourhood", "neighbourhood_key"),
    ("impact_at_time", "WeatherImpact", "TimeSlot", "time_key"),
    ("impact_on_date", "WeatherImpact", "CalendarDate", "date_key"),
    # StationSnapshot edges
    ("current_snapshot", "StationSnapshot", "Station", "station_key"),
    # RecentAvailability edges
    ("recent_at_station", "RecentAvailability", "Station", "station_key"),
    ("recent_at_time", "RecentAvailability", "TimeSlot", "time_key"),
    ("recent_on_date", "RecentAvailability", "CalendarDate", "date_key"),
    # DemandForecast edges
    ("forecast_for_zone", "DemandForecast", "Neighbourhood", "neighbourhood_key"),
    ("forecast_at_time", "DemandForecast", "TimeSlot", "time_key"),
    ("forecast_on_date", "DemandForecast", "CalendarDate", "date_key"),
    # Customer edges
    ("member_near_station", "Customer", "Neighbourhood", "Neighbourhood"),
    # WeatherObservation edges
    ("observed_during", "WeatherObservation", "TimeSlot", "time_key"),
]


# ============================================================
# API HELPERS
# ============================================================

def get_workspace_id(token: str, name: str) -> str:
    """Resolve workspace ID by display name."""
    url = f"{FABRIC_API_BASE}/workspaces"
    resp = requests.get(url, headers=get_auth_headers(token))
    resp.raise_for_status()
    for ws in resp.json().get("value", []):
        if ws["displayName"] == name:
            return ws["id"]
    available = [w["displayName"] for w in resp.json().get("value", [])]
    print(f"  [FAIL] Workspace '{name}' not found. Available: {available}")
    sys.exit(1)


def get_lakehouse_id(token: str, ws_id: str, name: str) -> str:
    """Resolve lakehouse ID by name."""
    url = f"{FABRIC_API_BASE}/workspaces/{ws_id}/lakehouses"
    resp = requests.get(url, headers=get_auth_headers(token))
    resp.raise_for_status()
    for lh in resp.json().get("value", []):
        if lh["displayName"] == name:
            return lh["id"]
    print(f"  [FAIL] Lakehouse '{name}' not found in workspace.")
    sys.exit(1)


def wait_for_lro(token: str, response, label: str, timeout: int = 300) -> bool:
    """Poll a long-running operation until completion."""
    headers = get_auth_headers(token)
    location = response.headers.get("Location")
    if not location:
        time.sleep(5)
        return True
    start = time.time()
    while time.time() - start < timeout:
        retry = int(response.headers.get("Retry-After", 5))
        time.sleep(retry)
        r = requests.get(location, headers=headers)
        if r.status_code == 200:
            status = r.json().get("status", "")
            if status == "Succeeded":
                return True
            if status in ("Failed", "Cancelled"):
                err = r.json().get("error", {}).get("message", "")
                print(f"    [FAIL] {label}: {status} — {err}")
                return False
        elif r.status_code == 404:
            time.sleep(3)
            return True
    print(f"    [FAIL] {label}: timed out after {timeout}s")
    return False


def find_existing_graph_model(token: str, ws_id: str, name: str) -> str | None:
    """Find an existing GraphModel by display name. Returns ID or None."""
    headers = get_auth_headers(token)
    # Try dedicated endpoint first
    url = f"{FABRIC_API_BASE}/workspaces/{ws_id}/graphModels"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        for gm in resp.json().get("value", []):
            if gm["displayName"] == name:
                return gm["id"]
    # Fallback to items endpoint
    url = f"{FABRIC_API_BASE}/workspaces/{ws_id}/items?type=GraphModel"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        for item in resp.json().get("value", []):
            if item["displayName"] == name:
                return item["id"]
    return None


# ============================================================
# GRAPH MODEL DEFINITION BUILDERS
# ============================================================

def _entity_lookup():
    """Build name → entity dict for quick lookup."""
    return {e["name"]: e for e in ENTITY_TYPES}


def build_graph_type():
    """
    Build graphType.json — the node/edge schema.
    NodeTypes: aliases, labels, primary keys, typed properties.
    EdgeTypes: aliases, labels, source/destination node references.
    """
    node_types = []
    for e in ENTITY_TYPES:
        props = [{"name": col, "type": TYPE_MAP.get(typ, "STRING")}
                 for col, typ in e["properties"]]
        node_types.append({
            "alias": f"{e['name']}_nodeType",
            "labels": [e["name"]],
            "primaryKeyProperties": [e["pk"]],
            "properties": props,
        })

    edge_types = []
    for rel_name, src, tgt, _ in RELATIONSHIPS:
        edge_types.append({
            "alias": f"{rel_name}_edgeType",
            "labels": [rel_name],
            "sourceNodeType": {"alias": f"{src}_nodeType"},
            "destinationNodeType": {"alias": f"{tgt}_nodeType"},
            "properties": [],
        })

    return {"schemaVersion": "1.0.0", "nodeTypes": node_types, "edgeTypes": edge_types}


def build_graph_definition():
    """
    Build graphDefinition.json — data mapping from tables to graph elements.
    NodeTables map entity properties to lakehouse columns.
    EdgeTables specify source/destination key columns for relationship joins.
    """
    entities = _entity_lookup()

    node_tables = []
    for e in ENTITY_TYPES:
        mappings = [{"propertyName": col, "sourceColumn": col}
                    for col, _ in e["properties"]]
        node_tables.append({
            "id": f"{e['name']}_{uuid.uuid4().hex[:8]}",
            "nodeTypeAlias": f"{e['name']}_nodeType",
            "dataSourceName": f"{e['table']}_Source",
            "propertyMappings": mappings,
        })

    edge_tables = []
    for rel_name, src_name, tgt_name, join_col in RELATIONSHIPS:
        src_entity = entities[src_name]
        tgt_entity = entities[tgt_name]
        # Edge data comes from the source entity's table
        # Source key = source PK column, Destination key = join column (FK)
        edge_tables.append({
            "id": f"{rel_name}_{uuid.uuid4().hex[:8]}",
            "edgeTypeAlias": f"{rel_name}_edgeType",
            "dataSourceName": f"{src_entity['table']}_Source",
            "sourceNodeKeyColumns": [src_entity["pk"]],
            "destinationNodeKeyColumns": [join_col],
            "propertyMappings": [],
        })

    return {"schemaVersion": "1.0.0", "nodeTables": node_tables, "edgeTables": edge_tables}


def build_data_sources(ws_id: str, lh_id: str):
    """
    Build dataSources.json — OneLake DFS paths for each delta table.
    Path: abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/<table>
    """
    tables = set()
    for e in ENTITY_TYPES:
        tables.add(e["table"])
    # Relationship edges use source entity tables (already included)

    sources = []
    for tbl in sorted(tables):
        path = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Tables/{tbl}"
        sources.append({
            "name": f"{tbl}_Source",
            "type": "DeltaTable",
            "properties": {"path": path},
        })

    return {"dataSources": sources}


def build_styling():
    """Build stylingConfiguration.json — circular layout for graph canvas."""
    positions = {}
    styles = {}
    n = len(ENTITY_TYPES)
    radius = 350

    for i, e in enumerate(ENTITY_TYPES):
        angle = 2 * math.pi * i / n
        alias = f"{e['name']}_nodeType"
        positions[alias] = {
            "x": int(radius + radius * math.cos(angle)),
            "y": int(radius + radius * math.sin(angle)),
        }
        styles[alias] = {"size": 30}

    return {
        "schemaVersion": "1.0.0",
        "modelLayout": {
            "positions": positions,
            "styles": styles,
            "pan": {"x": 0, "y": 0},
            "zoomLevel": 1,
        },
    }


# ============================================================
# DEPLOY LOGIC
# ============================================================

def encode_part(path: str, content: dict) -> dict:
    """Base64-encode a JSON object as a definition part."""
    raw = json.dumps(content, indent=2)
    b64 = base64.b64encode(raw.encode("utf-8")).decode("utf-8")
    return {"path": path, "payload": b64, "payloadType": "InlineBase64"}


def build_definition_parts(ws_id: str, lh_id: str):
    """Build the 4 GraphModel definition parts."""
    print("\n  Step 3: Generating GraphModel definition parts")

    gt = build_graph_type()
    gd = build_graph_definition()
    ds = build_data_sources(ws_id, lh_id)
    sc = build_styling()

    print(f"    graphType.json:           {len(gt['nodeTypes'])} nodeTypes, {len(gt['edgeTypes'])} edgeTypes")
    print(f"    graphDefinition.json:     {len(gd['nodeTables'])} nodeTables, {len(gd['edgeTables'])} edgeTables")
    print(f"    dataSources.json:         {len(ds['dataSources'])} delta tables")
    print(f"    stylingConfiguration.json: {len(sc['modelLayout']['positions'])} node positions")

    parts = [
        encode_part("graphType.json", gt),
        encode_part("graphDefinition.json", gd),
        encode_part("dataSources.json", ds),
        encode_part("stylingConfiguration.json", sc),
    ]
    return parts


def create_graph_model(token: str, ws_id: str, name: str, description: str,
                       parts: list) -> str | None:
    """Create a new GraphModel with a full definition. Returns item ID."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{ws_id}/graphModels"
    body = {
        "displayName": name,
        "description": description,
        "definition": {
            "format": "json",
            "parts": parts,
        },
    }

    resp = requests.post(url, headers=headers, json=body)
    if resp.status_code == 201:
        item_id = resp.json().get("id")
        print(f"    [OK] Created GraphModel: {item_id}")
        return item_id
    elif resp.status_code == 202:
        print(f"    [OK] Creation accepted (async)...")
        ok = wait_for_lro(token, resp, name)
        if ok:
            gm_id = find_existing_graph_model(token, ws_id, name)
            if gm_id:
                print(f"    [OK] Resolved GraphModel ID: {gm_id}")
                return gm_id
        return None
    else:
        print(f"    [FAIL] Create GraphModel: {resp.status_code}")
        print(f"    {resp.text[:500]}")
        return None


def update_graph_model_definition(token: str, ws_id: str, gm_id: str,
                                  parts: list) -> bool:
    """Push an updated definition to an existing GraphModel."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{ws_id}/graphModels/{gm_id}/updateDefinition"
    body = {
        "definition": {
            "format": "json",
            "parts": parts,
        },
    }
    resp = requests.post(url, headers=headers, json=body)
    if resp.status_code == 200:
        print(f"    [OK] Definition updated (200)")
        return True
    elif resp.status_code == 202:
        print(f"    [OK] Definition update accepted (async)...")
        return wait_for_lro(token, resp, "updateDefinition")
    else:
        print(f"    [FAIL] updateDefinition: {resp.status_code}")
        print(f"    {resp.text[:500]}")
        return False


def check_graph_status(token: str, ws_id: str, gm_id: str):
    """Print the graph model's loading status and query readiness."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{ws_id}/graphModels/{gm_id}"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        props = resp.json().get("properties", {})
        readiness = props.get("queryReadiness", "Unknown")
        loading = props.get("lastDataLoadingStatus") or {}
        status = loading.get("status", "Unknown")
        print(f"    queryReadiness:    {readiness}")
        print(f"    loadingStatus:     {status}")
        return readiness, status
    return "Unknown", "Unknown"


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Deploy Bicycle Fleet GraphModel")
    parser.add_argument("--workspace", default=WORKSPACE_NAME, help="Target workspace name")
    parser.add_argument("--graph-model", default=GRAPH_MODEL_NAME, help="GraphModel display name")
    parser.add_argument("--update", action="store_true", help="Update existing GraphModel definition")
    parser.add_argument("--no-refresh", action="store_true", help="Skip waiting for data load")
    args = parser.parse_args()

    print("=" * 70)
    print("  DEPLOY BICYCLE FLEET GRAPH MODEL")
    print("=" * 70)
    print()
    print(f"  Target workspace: {args.workspace}")
    print(f"  GraphModel name:  {args.graph_model}")
    print(f"  Lakehouse:        {LAKEHOUSE_NAME}")
    print(f"  Entity types:     {len(ENTITY_TYPES)}")
    print(f"  Relationships:    {len(RELATIONSHIPS)}")
    print()

    # ── Step 1: Authenticate & resolve targets ──────────────
    print("  Step 1: Authenticate & resolve workspace")
    token = get_fabric_token(TENANT_ID, ADMIN_ACCOUNT)
    ws_id = get_workspace_id(token, args.workspace)
    print(f"    Workspace: {args.workspace} ({ws_id})")

    lh_id = get_lakehouse_id(token, ws_id, LAKEHOUSE_NAME)
    print(f"    Lakehouse: {LAKEHOUSE_NAME} ({lh_id})")

    # ── Step 2: Check for existing GraphModel ─────────────
    print("\n  Step 2: Check for existing GraphModel")
    existing_id = find_existing_graph_model(token, ws_id, args.graph_model)
    if existing_id:
        print(f"    Found existing: {args.graph_model} ({existing_id})")
    else:
        print(f"    No existing GraphModel named '{args.graph_model}'")

    # ── Step 3: Generate definition ───────────────────────
    parts = build_definition_parts(ws_id, lh_id)

    # ── Step 4: Deploy ────────────────────────────────────
    if existing_id and args.update:
        print(f"\n  Step 4: Updating existing GraphModel ({existing_id})")
        ok = update_graph_model_definition(token, ws_id, existing_id, parts)
        gm_id = existing_id if ok else None
        result = "[OK] Definition updated" if ok else "[FAIL] Update failed"
    elif existing_id and not args.update:
        print(f"\n  Step 4: GraphModel '{args.graph_model}' already exists ({existing_id})")
        print(f"    Use --update to overwrite the definition.")
        gm_id = existing_id
        result = "[SKIP] Already exists"
    else:
        print(f"\n  Step 4: Creating GraphModel '{args.graph_model}'")
        description = (
            f"Graph model for London bike-share fleet intelligence. "
            f"Contains {len(ENTITY_TYPES)} node types (Station, Neighbourhood, "
            f"WeatherObservation, DemandForecast, etc.) and {len(RELATIONSHIPS)} "
            f"edge types mapped to bicycles_gold lakehouse delta tables."
        )
        gm_id = create_graph_model(token, ws_id, args.graph_model, description, parts)
        result = f"[OK] Created ({gm_id})" if gm_id else "[FAIL] Create failed"

    # ── Step 5: Wait for data load ──────────────────────
    if gm_id and not args.no_refresh and "FAIL" not in result and "SKIP" not in result:
        print(f"\n  Step 5: Waiting for data load (auto-triggered on create)")
        for i in range(40):  # up to ~20 min
            readiness, load_status = check_graph_status(token, ws_id, gm_id)
            if readiness == "Full" and load_status == "Completed":
                print(f"    Data load complete.")
                result += " + data loaded"
                break
            if load_status == "Failed":
                print(f"    [WARN] Data load failed — check job history in Fabric portal.")
                break
            time.sleep(30)
        else:
            print(f"    [WARN] Timed out waiting for data load. It may still be running.")

    # ── Step 6: Final status ──────────────────────────────
    if gm_id:
        print(f"\n  Step 6: Graph model status")
        check_graph_status(token, ws_id, gm_id)

    # ── Summary ───────────────────────────────────────────
    print()
    print("=" * 70)
    print("  DEPLOYMENT SUMMARY")
    print("=" * 70)
    print(f"  {args.graph_model:<45} {result}")
    print()
    print("  Graph model definition:")
    print(f"    Nodes: {len(ENTITY_TYPES)} types — {', '.join(e['name'] for e in ENTITY_TYPES)}")
    print(f"    Edges: {len(RELATIONSHIPS)} types — {', '.join(r[0] for r in RELATIONSHIPS[:5])}...")
    tables = sorted(set(e["table"] for e in ENTITY_TYPES))
    print(f"    Tables: {len(tables)} delta tables — {', '.join(tables[:4])}...")
    print()
    print("  " + "-" * 66)
    print("  NOTE: This is a STANDALONE GraphModel, independent of the Ontology.")
    print("  The Fabric Preview API does not support linking Ontology ↔ GraphModel.")
    print("  The graph uses the same tables and schema as the ontology.")
    print()

    if "FAIL" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
