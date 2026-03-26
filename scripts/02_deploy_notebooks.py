"""
Deploy Notebooks to Microsoft Fabric
=====================================
Deploys the Bicycle Real-Time Intelligence notebooks (.ipynb) into a
"Notebooks" workspace folder in the target Fabric workspace.

Notebooks deployed (in pipeline order):
  01_Event_Simulator              -- Generate realistic bicycle telemetry events
  02_Bronze_Streaming_Ingest      -- Data quality checks & reconciliation
  03_Silver_Enrich_Transform      -- Bronze -> Silver enrichment & profiling
  03a_Silver_Weather_Join         -- Weather flatten, hourly dedup, bike-weather join
  04_Gold_Star_Schema             -- Silver -> Gold star schema (5 dims, 4 facts)
  05_KQL_Realtime_Queries         -- 8 KQL queries for Eventhouse dashboards
  06_ML_Demand_Forecast           -- ML demand forecasting (Random Forest + weather features)
  07_Activator_Alerts             -- Alert rules & operations automation
  08_GeoAnalytics_HotSpots        -- ArcGIS spatial hot spot analysis & rebalancing routes

Usage:
    python 02_deploy_notebooks.py
    python 02_deploy_notebooks.py --update          # Update existing notebooks
"""

import requests
import sys
import time
import base64
import json
from pathlib import Path
from fabric_auth import get_fabric_token, get_auth_headers
import argparse
from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

# Ensure stdout handles any stray Unicode on Windows cmd.exe / older PowerShell
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ============================================================
# CONFIGURATION
# ============================================================
ACCOUNT = ADMIN_ACCOUNT

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

# Workspace folder to place notebooks in
NOTEBOOK_FOLDER_NAME = "Notebooks"

# Source directory for .ipynb files
NOTEBOOKS_SOURCE_DIR = Path(__file__).parent / "notebooks"

# Default lakehouse to attach to all notebooks (for pipeline execution)
LAKEHOUSE_ID = "b05b765b-b136-4a6f-b56f-9ab928fb3e78"
LAKEHOUSE_NAME = "bicycles_gold"
WORKSPACE_ID = "573cc7c7-a45a-4fd9-886e-9db4e9798db4"

# Notebooks to deploy -- ordered by pipeline execution sequence
NOTEBOOKS = [
    {
        "file": "01_Event_Simulator.ipynb",
        "name": "01_Event_Simulator",
        "description": "Bicycle RTI -- Generate realistic station telemetry events (docks, bikes, utilization, weather, maintenance)",
    },
    {
        "file": "02_Bronze_Streaming_Ingest.ipynb",
        "name": "02_Bronze_Streaming_Ingest",
        "description": "Bicycle RTI -- Data quality checks on raw data, streaming event validation, station reference table",
    },
    {
        "file": "03_Silver_Enrich_Transform.ipynb",
        "name": "03_Silver_Enrich_Transform",
        "description": "Bicycle RTI -- Bronze -> Silver: station profiles, availability bands, neighbourhood metrics, rebalancing candidates, hourly demand",
    },
    {
        "file": "03a_Silver_Weather_Join.ipynb",
        "name": "03a_Silver_Weather_Join",
        "description": "Bicycle RTI -- Weather flatten + bike-weather join: flattens AccuWeather structs, deduplicates to hourly, joins with bike demand, adds cycling comfort index and weather severity",
    },
    {
        "file": "04_Gold_Star_Schema.ipynb",
        "name": "04_Gold_Star_Schema",
        "description": "Bicycle RTI -- Silver -> Gold: star schema with dim_station, dim_neighbourhood, dim_time, dim_date, dim_weather, fact_availability, fact_hourly_demand, fact_rebalancing, fact_weather_impact",
    },
    {
        "file": "05_KQL_Realtime_Queries.ipynb",
        "name": "05_KQL_Realtime_Queries",
        "description": "Bicycle RTI -- 8 KQL real-time queries: live status, rebalance alerts, demand trends, anomaly detection, SLA monitoring, dynamic pricing",
    },
    {
        "file": "06_ML_Demand_Forecast.ipynb",
        "name": "06_ML_Demand_Forecast",
        "description": "Bicycle RTI -- ML: Random Forest demand forecasting per neighbourhood (24h ahead) with confidence bands and pre-positioning signals",
    },
    {
        "file": "07_Activator_Alerts.ipynb",
        "name": "07_Activator_Alerts",
        "description": "Bicycle RTI -- Activator alert configuration: 10 alert rules, alert evaluation, alert log, forecast-based alerts, operations automation",
    },
    {
        "file": "08_GeoAnalytics_HotSpots.ipynb",
        "name": "08_GeoAnalytics_HotSpots",
        "description": "Bicycle RTI -- ArcGIS GeoAnalytics: Getis-Ord Gi* hot spot analysis, neighbourhood risk zones, proximity-based rebalancing routes",
    },
    {
        "file": "09_Ontology_Neighbourhood_Filter.ipynb",
        "name": "09_Ontology_Neighbourhood_Filter",
        "description": "Bicycle RTI -- Creates neighbourhood-scoped onto_* tables for the ontology graph. Filters large fact tables to a single neighbourhood so GraphQL traversals work within timeout limits.",
    },
]


# ============================================================
# API HELPERS
# ============================================================

def get_workspace_id(token: str, workspace_name: str) -> str:
    """Get workspace ID by name."""
    url = f"{FABRIC_API_BASE}/workspaces"
    headers = get_auth_headers(token)

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"  [FAIL] Failed to list workspaces: {response.status_code}")
        sys.exit(1)

    for ws in response.json().get("value", []):
        if ws["displayName"] == workspace_name:
            return ws["id"]

    available = [w["displayName"] for w in response.json().get("value", [])]
    print(f"  [FAIL] Workspace '{workspace_name}' not found")
    print(f"    Available: {available}")
    sys.exit(1)


def get_or_create_folder(token: str, workspace_id: str, folder_name: str) -> str | None:
    """Get or create a workspace folder. Returns the folder ID."""
    headers = get_auth_headers(token)

    # Check if folder already exists
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/folders"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        for folder in response.json().get("value", []):
            if folder["displayName"] == folder_name:
                print(f"  [WARN] Folder '{folder_name}' already exists (ID: {folder['id']})")
                return folder["id"]

    # Create the folder
    payload = {"displayName": folder_name}
    print(f"  Creating folder '{folder_name}'...")
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code in (200, 201):
        folder_id = response.json()["id"]
        print(f"  [OK] Folder '{folder_name}' created (ID: {folder_id})")
        return folder_id
    elif response.status_code == 409 or "FolderDisplayNameAlreadyInUse" in response.text:
        print(f"  [WARN] Folder '{folder_name}' already exists (conflict)")
        resp2 = requests.get(f"{FABRIC_API_BASE}/workspaces/{workspace_id}/folders", headers=headers)
        if resp2.status_code == 200:
            for folder in resp2.json().get("value", []):
                if folder["displayName"] == folder_name:
                    return folder["id"]
        return None
    else:
        print(f"  [FAIL] Failed to create folder: {response.status_code}")
        print(f"    {response.text[:300]}")
        return None


def list_existing_notebooks(token: str, workspace_id: str) -> list[dict]:
    """List all notebooks in the workspace."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=Notebook"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("value", [])
    return []


def find_notebook_by_name(notebooks: list[dict], name: str) -> dict | None:
    """Find notebook by display name."""
    for nb in notebooks:
        if nb.get("displayName") == name:
            return nb
    return None


def wait_for_long_running_operation(token: str, response: requests.Response, item_name: str, timeout_seconds: int = 120) -> bool:
    """Poll a long-running operation until completion."""
    headers = get_auth_headers(token)
    operation_url = response.headers.get("Location")

    if not operation_url:
        print(f"    Waiting for {item_name} (no operation URL)...")
        time.sleep(10)
        return True

    print(f"    Provisioning {item_name}...")
    start = time.time()
    while time.time() - start < timeout_seconds:
        time.sleep(3)
        op_resp = requests.get(operation_url, headers=headers)
        if op_resp.status_code == 200:
            status = op_resp.json().get("status", "")
            if status == "Succeeded":
                return True
            elif status in ("Failed", "Cancelled"):
                print(f"    [FAIL] {item_name} provisioning {status}")
                return False
        elif op_resp.status_code == 404:
            # Operation URL disappeared -- item may have been created
            time.sleep(3)
            return True

    print(f"    [FAIL] {item_name} timed out ({timeout_seconds}s)")
    return False


def read_notebook_content(filepath: Path) -> str:
    """Read .ipynb file and return base64-encoded content."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Validate it's valid JSON (Jupyter notebook format)
    try:
        json.loads(content)
    except json.JSONDecodeError:
        print(f"    [WARN] {filepath.name} is not valid JSON -- might be VSCode XML format")
        content = convert_vscode_to_jupyter(content, filepath.name)

    return base64.b64encode(content.encode("utf-8")).decode("utf-8")


def convert_vscode_to_jupyter(xml_content: str, filename: str) -> str:
    """
    Convert VSCode XML notebook format (<VSCode.Cell> tags) to standard
    Jupyter .ipynb JSON format that Fabric can ingest.
    """
    import re

    cells = []
    pattern = re.compile(
        r'<VSCode\.Cell[^>]*language="(\w+)"[^>]*>(.*?)</VSCode\.Cell>',
        re.DOTALL,
    )

    for match in pattern.finditer(xml_content):
        lang = match.group(1)
        source = match.group(2)

        cell_type = "markdown" if lang == "markdown" else "code"

        lines = source.split("\n")
        while lines and lines[0].strip() == "":
            lines.pop(0)
        while lines and lines[-1].strip() == "":
            lines.pop()

        source_lines = [line + "\n" for line in lines]
        if source_lines:
            source_lines[-1] = source_lines[-1].rstrip("\n")

        cell = {
            "cell_type": cell_type,
            "source": source_lines,
            "metadata": {},
        }
        if cell_type == "code":
            cell["outputs"] = []
            cell["execution_count"] = None

        cells.append(cell)

    notebook = {
        "nbformat": 4,
        "nbformat_minor": 2,
        "metadata": {
            "kernel_info": {"name": "synapse_pyspark"},
            "language_info": {"name": "python"},
            "trident": {
                "lakehouse": {
                    "default_lakehouse": LAKEHOUSE_ID,
                    "default_lakehouse_name": LAKEHOUSE_NAME,
                    "default_lakehouse_workspace_id": WORKSPACE_ID,
                    "known_lakehouses": [
                        {
                            "id": LAKEHOUSE_ID,
                        }
                    ],
                }
            },
            "dependencies": {
                "lakehouse": {
                    "default_lakehouse": LAKEHOUSE_ID,
                    "default_lakehouse_name": LAKEHOUSE_NAME,
                    "default_lakehouse_workspace_id": WORKSPACE_ID,
                    "known_lakehouses": [
                        {
                            "id": LAKEHOUSE_ID,
                        }
                    ],
                }
            },
        },
        "cells": cells,
    }

    print(f"    Converted {filename}: {len(cells)} cells")
    return json.dumps(notebook)


# ============================================================
# CREATE / UPDATE NOTEBOOK
# ============================================================

def create_notebook(
    token: str, workspace_id: str, name: str, description: str,
    content_b64: str, folder_id: str | None = None,
) -> str | None:
    """Create a new notebook. Returns notebook ID."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"

    payload = {
        "type": "Notebook",
        "displayName": name,
        "description": description,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": content_b64,
                    "payloadType": "InlineBase64",
                }
            ],
        },
    }

    if folder_id:
        payload["folderId"] = folder_id

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code in (200, 201):
        notebook_id = response.json().get("id")
        print(f"  [OK] Created: {name} (ID: {notebook_id})")
        return notebook_id

    elif response.status_code == 202:
        success = wait_for_long_running_operation(token, response, name)
        if success:
            notebooks = list_existing_notebooks(token, workspace_id)
            nb = find_notebook_by_name(notebooks, name)
            if nb:
                print(f"  [OK] Created: {name} (ID: {nb['id']})")
                return nb["id"]
        print(f"  [FAIL] Failed to create: {name}")
        return None

    elif response.status_code == 409:
        print(f"  [WARN] Already exists: {name}")
        return "exists"

    else:
        print(f"  [FAIL] Failed: {name} -- HTTP {response.status_code}")
        print(f"    {response.text[:300]}")
        return None


def update_notebook(
    token: str, workspace_id: str, item_id: str, name: str,
    content_b64: str,
) -> bool:
    """Update an existing notebook's definition. Returns True on success."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{item_id}/updateDefinition"

    payload = {
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": content_b64,
                    "payloadType": "InlineBase64",
                }
            ],
        }
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code in (200, 201):
        print(f"  [OK] Updated: {name}")
        return True
    elif response.status_code == 202:
        success = wait_for_long_running_operation(token, response, name)
        if success:
            print(f"  [OK] Updated: {name}")
        else:
            print(f"  [FAIL] Update failed: {name}")
        return success
    else:
        print(f"  [FAIL] Update failed: {name} -- HTTP {response.status_code}")
        print(f"    {response.text[:300]}")
        return False


# ============================================================
# MAIN
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(description="Deploy Bicycle RTI notebooks to Fabric")
    parser.add_argument(
        "--update", action="store_true",
        help="Update existing notebooks (re-upload definition). Default is create-only.",
    )
    return parser.parse_args()


def main():
    """Deploy notebooks to Fabric workspace."""
    args = parse_args()

    print("=" * 70)
    print("BICYCLE REAL-TIME INTELLIGENCE -- NOTEBOOK DEPLOYMENT")
    print("=" * 70)
    print(f"  Tenant:    {TENANT_ID}")
    print(f"  Account:   {ACCOUNT}")
    print(f"  Workspace: {WORKSPACE_NAME}")
    print(f"  Source:     {NOTEBOOKS_SOURCE_DIR}")
    print(f"  Mode:      {'Update existing' if args.update else 'Create new'}")
    print()

    # Verify source directory exists
    if not NOTEBOOKS_SOURCE_DIR.exists():
        print(f"[FAIL] Notebooks source directory not found: {NOTEBOOKS_SOURCE_DIR}")
        sys.exit(1)

    # Authenticate
    print("Step 1: Authenticating...")
    token = get_fabric_token(TENANT_ID, ACCOUNT)
    print("  [OK] Token acquired\n")

    # Get workspace ID
    print("Step 2: Finding workspace...")
    workspace_id = get_workspace_id(token, WORKSPACE_NAME)
    print(f"  [OK] Workspace ID: {workspace_id}\n")

    # Create or get Notebooks folder
    print("Step 3: Creating workspace folder...")
    folder_id = get_or_create_folder(token, workspace_id, NOTEBOOK_FOLDER_NAME)
    print()

    # List existing notebooks (for update/skip logic)
    print("Step 4: Checking existing notebooks...")
    existing_notebooks = list_existing_notebooks(token, workspace_id)
    print(f"  Found {len(existing_notebooks)} notebook(s) in workspace\n")

    # Deploy each notebook
    print(f"Step 5: Deploying {len(NOTEBOOKS)} notebooks...")
    print("-" * 50)

    results = {}

    for nb_info in NOTEBOOKS:
        filepath = NOTEBOOKS_SOURCE_DIR / nb_info["file"]
        name = nb_info["name"]
        desc = nb_info["description"]

        # Check source file exists
        if not filepath.exists():
            print(f"  [FAIL] Source not found: {filepath}")
            results[name] = "[FAIL] Source file missing"
            continue

        # Read and encode content
        content_b64 = read_notebook_content(filepath)

        # Check if notebook already exists
        existing = find_notebook_by_name(existing_notebooks, name)

        if existing and args.update:
            print(f"  Updating: {name}...")
            ok = update_notebook(token, workspace_id, existing["id"], name, content_b64)
            results[name] = "[OK] Updated" if ok else "[FAIL] Update failed"

        elif existing and not args.update:
            print(f"  [WARN] Exists: {name} (use --update to overwrite)")
            results[name] = "[WARN] Already exists"

        else:
            print(f"  Creating: {name}...")
            nb_id = create_notebook(
                token, workspace_id, name, desc, content_b64,
                folder_id=folder_id,
            )
            if nb_id:
                results[name] = "[OK] Created"
            else:
                results[name] = "[FAIL] Failed"

        # Small delay to avoid API throttling
        time.sleep(2)

    # ── Summary ────────────────────────────────────────────
    print()
    print("=" * 70)
    print("DEPLOYMENT SUMMARY")
    print("=" * 70)
    for name, status in results.items():
        print(f"  {name:<40} {status}")

    created = sum(1 for v in results.values() if "Created" in v or "Updated" in v)
    failed = sum(1 for v in results.values() if "[FAIL]" in v)
    skipped = sum(1 for v in results.values() if "[WARN]" in v)

    print()
    print(f"  Total: {created} deployed, {skipped} skipped, {failed} failed")
    print()

    print("PIPELINE EXECUTION ORDER (run manually in Fabric):")
    print("-" * 60)
    print("  1. 01_Event_Simulator              -> bikerental_bronze_raw (bicycle_events)")
    print("  2. 02_Bronze_Streaming_Ingest      -> bikerental_bronze_raw (quality checks)")
    print("  3. 03_Silver_Enrich_Transform      -> bicycles_silver (5 silver tables)")
    print("  4. 04_Gold_Star_Schema             -> bicycles_gold (4 dims + 3 facts)")
    print("  5. 05_KQL_Realtime_Queries         -> bikerentaleventhouse (8 KQL queries)")
    print("  6. 06_ML_Demand_Forecast           -> bicycles_gold (forecast_demand)")
    print("  7. 07_Activator_Alerts             -> bicycles_gold (alert_configuration + alert_log)")
    print()
    print("REAL-TIME STREAMING SETUP (manual in Fabric UI):")
    print("-" * 60)
    print("  1. Create Eventstream: bicycle_event_stream")
    print("  2. Source: Custom App (or event simulator notebook)")
    print("  3. Destination 1: bikerentaleventhouse -> bikerentaldb")
    print("  4. Destination 2: bikerental_bronze_raw -> bicycle_events_delta")
    print("  5. Create Activator (Reflex) using alert rules from notebook 07")
    print()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
