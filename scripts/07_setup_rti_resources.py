"""
Setup Real-Time Intelligence Resources — Bicycle Fleet Streaming
==================================================================
Creates the Fabric RTI infrastructure for bicycle fleet real-time monitoring:

  1. Eventhouse:   bikerentaleventhouse
  2. KQL Database: bikerentaldb
  3. KQL Tables:   bikerentaldb (station events), weather_tb (weather data)

The Eventstream must be created in the Fabric UI because the API for
Eventstream custom endpoints requires UI-based activation. This script
creates the Eventhouse + KQL DB + Tables, then prints step-by-step
instructions for the Eventstream setup.

Prerequisites:
  - .env configured with TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME
  - pip install -r requirements.txt

Usage:
    python 07_setup_rti_resources.py
"""

import sys
import time
import json
import requests
from fabric_auth import get_fabric_token, get_auth_headers
from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ============================================================
# CONFIGURATION
# ============================================================

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
EVENTHOUSE_NAME = "bikerentaleventhouse"
EVENTHOUSE_DESCRIPTION = (
    "Real-Time Intelligence hub — KQL database for streaming bicycle "
    "station telemetry, weather data, and operational dashboards"
)
KQL_DB_NAME = "bikerentaldb"
KQL_DB_DESCRIPTION = (
    "KQL database for real-time bicycle station queries, availability "
    "alerting, and RTI dashboard tiles"
)
EVENTSTREAM_NAME = "bicycles_eventstream"

# KQL table creation commands
# These match the column names used in dashboard.json queries
KQL_TABLE_COMMANDS = [
    # ── Main station events table ──
    # Column names match what the dashboard queries expect
    """
.create-merge table bikerentaldb (
    BikepointID: string,
    Street: string,
    Neighbourhood: string,
    Latitude: real,
    Longitude: real,
    No_Bikes: int,
    No_Empty_Docks: int,
    event_timestamp: datetime
)
""",
    # ── Ingestion mapping for JSON events ──
    """
.create-or-alter table bikerentaldb ingestion json mapping 'BikeEventsMapping'
'['
'  {"column": "BikepointID",     "path": "$.BikepointID",     "datatype": "string"},'
'  {"column": "Street",          "path": "$.Street",          "datatype": "string"},'
'  {"column": "Neighbourhood",   "path": "$.Neighbourhood",   "datatype": "string"},'
'  {"column": "Latitude",        "path": "$.Latitude",        "datatype": "real"},'
'  {"column": "Longitude",       "path": "$.Longitude",       "datatype": "real"},'
'  {"column": "No_Bikes",        "path": "$.No_Bikes",        "datatype": "int"},'
'  {"column": "No_Empty_Docks",  "path": "$.No_Empty_Docks",  "datatype": "int"},'
'  {"column": "event_timestamp", "path": "$.event_timestamp", "datatype": "datetime"}'
']'
""",
    # ── Weather observations table ──
    """
.create-merge table weather_tb (
    temperature: real,
    realFeelTemperature: real,
    description: string,
    windSpeed: real,
    humidity: real,
    event_timestamp: datetime
)
""",
    # ── Weather ingestion mapping ──
    """
.create-or-alter table weather_tb ingestion json mapping 'WeatherMapping'
'['
'  {"column": "temperature",        "path": "$.temperature",        "datatype": "real"},'
'  {"column": "realFeelTemperature","path": "$.realFeelTemperature","datatype": "real"},'
'  {"column": "description",        "path": "$.description",        "datatype": "string"},'
'  {"column": "windSpeed",          "path": "$.windSpeed",          "datatype": "real"},'
'  {"column": "humidity",           "path": "$.humidity",           "datatype": "real"},'
'  {"column": "event_timestamp",    "path": "$.event_timestamp",    "datatype": "datetime"}'
']'
""",
    # ── Retention policy (90 days for demo) ──
    ".alter-merge table bikerentaldb policy retention softdelete = 90d",
    ".alter-merge table weather_tb policy retention softdelete = 90d",
    # ── Enable streaming ingestion for low-latency ──
    ".alter table bikerentaldb policy streamingingestion enable",
    ".alter table weather_tb policy streamingingestion enable",
    # ── Caching policy: hot cache last 7 days ──
    ".alter table bikerentaldb policy caching hot = 7d",
    ".alter table weather_tb policy caching hot = 7d",
]


# ============================================================
# API HELPERS
# ============================================================

def get_workspace_id(token: str) -> str:
    url = f"{FABRIC_API_BASE}/workspaces"
    resp = requests.get(url, headers=get_auth_headers(token))
    resp.raise_for_status()
    for ws in resp.json().get("value", []):
        if ws["displayName"] == WORKSPACE_NAME:
            return ws["id"]
    available = [w["displayName"] for w in resp.json().get("value", [])]
    print(f"  [FAIL] Workspace '{WORKSPACE_NAME}' not found")
    print(f"    Available: {available}")
    sys.exit(1)


def get_item(token: str, workspace_id: str, item_type: str, name: str):
    """Find a Fabric item by type and display name."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type={item_type}"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        for item in resp.json().get("value", []):
            if item["displayName"] == name:
                return item
    return None


def wait_for_operation(token: str, response, label: str,
                       timeout_seconds: int = 180) -> bool:
    """Wait for an async Fabric operation to complete."""
    headers = get_auth_headers(token)
    location = response.headers.get("Location")
    if not location:
        time.sleep(5)
        return True

    print(f"    ... Provisioning {label}...")
    start = time.time()
    while time.time() - start < timeout_seconds:
        retry_after = int(response.headers.get("Retry-After", 5))
        time.sleep(retry_after)
        op_resp = requests.get(location, headers=headers)
        if op_resp.status_code == 200:
            status = op_resp.json().get("status", "")
            if status == "Succeeded":
                return True
            elif status in ("Failed", "Cancelled"):
                error = op_resp.json().get("error", {}).get("message", "")
                print(f"    [FAIL] {label}: {status} — {error}")
                return False
    print(f"    [FAIL] {label} timed out ({timeout_seconds}s)")
    return False


def create_eventhouse(token: str, workspace_id: str) -> str | None:
    """Create Eventhouse if not exists. Returns eventhouse ID."""
    headers = get_auth_headers(token)

    # Check existing
    existing = get_item(token, workspace_id, "Eventhouse", EVENTHOUSE_NAME)
    if existing:
        print(f"  [OK] Eventhouse '{EVENTHOUSE_NAME}' already exists (ID: {existing['id']})")
        return existing["id"]

    # Create
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/eventhouses"
    payload = {
        "displayName": EVENTHOUSE_NAME,
        "description": EVENTHOUSE_DESCRIPTION,
    }
    print(f"  Creating Eventhouse '{EVENTHOUSE_NAME}'...")
    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code == 201:
        eh_id = resp.json().get("id")
        print(f"  [OK] Eventhouse '{EVENTHOUSE_NAME}' created (ID: {eh_id})")
        return eh_id
    elif resp.status_code == 202:
        success = wait_for_operation(token, resp, EVENTHOUSE_NAME)
        if success:
            item = get_item(token, workspace_id, "Eventhouse", EVENTHOUSE_NAME)
            if item:
                print(f"  [OK] Eventhouse '{EVENTHOUSE_NAME}' created (ID: {item['id']})")
                return item["id"]
    elif resp.status_code == 409:
        print(f"  [OK] Eventhouse '{EVENTHOUSE_NAME}' already exists")
        existing = get_item(token, workspace_id, "Eventhouse", EVENTHOUSE_NAME)
        return existing["id"] if existing else None
    else:
        print(f"  [FAIL] Create Eventhouse: HTTP {resp.status_code}")
        print(f"    {resp.text[:400]}")

    return None


def create_kql_database(token: str, workspace_id: str, eventhouse_id: str) -> str | None:
    """Create KQL Database inside the Eventhouse. Returns database ID."""
    headers = get_auth_headers(token)

    # Check existing
    existing = get_item(token, workspace_id, "KQLDatabase", KQL_DB_NAME)
    if existing:
        print(f"  [OK] KQL Database '{KQL_DB_NAME}' already exists (ID: {existing['id']})")
        return existing["id"]

    # Create
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/kqlDatabases"
    payload = {
        "displayName": KQL_DB_NAME,
        "description": KQL_DB_DESCRIPTION,
        "creationPayload": {
            "databaseType": "ReadWrite",
            "parentEventhouseItemId": eventhouse_id,
        },
    }
    print(f"  Creating KQL Database '{KQL_DB_NAME}'...")
    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code == 201:
        db_id = resp.json().get("id")
        print(f"  [OK] KQL Database '{KQL_DB_NAME}' created (ID: {db_id})")
        return db_id
    elif resp.status_code == 202:
        success = wait_for_operation(token, resp, KQL_DB_NAME)
        if success:
            item = get_item(token, workspace_id, "KQLDatabase", KQL_DB_NAME)
            if item:
                print(f"  [OK] KQL Database '{KQL_DB_NAME}' created (ID: {item['id']})")
                return item["id"]
    elif resp.status_code == 409:
        print(f"  [OK] KQL Database '{KQL_DB_NAME}' already exists")
        existing = get_item(token, workspace_id, "KQLDatabase", KQL_DB_NAME)
        return existing["id"] if existing else None
    else:
        print(f"  [FAIL] Create KQL Database: HTTP {resp.status_code}")
        print(f"    {resp.text[:400]}")

    return None


def get_kql_connection_string(token: str, workspace_id: str, kql_db_id: str) -> str | None:
    """Get the KQL database connection string for running management commands."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/kqlDatabases/{kql_db_id}"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        props = resp.json().get("properties", {})
        return props.get("queryUri") or props.get("parentEventhouseUri")
    return None


def run_kql_command(token: str, cluster_uri: str, database: str, command: str) -> bool:
    """Execute a KQL management command."""
    if not cluster_uri:
        print(f"    [SKIP] No cluster URI for command")
        return False

    # Trim command and skip empty
    command = command.strip()
    if not command:
        return True

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    url = f"{cluster_uri}/v1/rest/mgmt"
    payload = {
        "db": database,
        "csl": command,
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        if resp.status_code == 200:
            return True
        else:
            print(f"    [WARN] KQL command failed: HTTP {resp.status_code}")
            print(f"      Command: {command[:80]}...")
            return False
    except requests.exceptions.RequestException as e:
        print(f"    [WARN] KQL command error: {e}")
        return False


def setup_kql_tables(token: str, cluster_uri: str, database: str):
    """Create KQL tables and ingestion mappings."""
    print(f"  Setting up KQL tables in '{database}'...")
    success_count = 0
    for cmd in KQL_TABLE_COMMANDS:
        cmd = cmd.strip()
        if not cmd:
            continue
        ok = run_kql_command(token, cluster_uri, database, cmd)
        if ok:
            # Extract table name for logging
            if ".create-merge table" in cmd:
                table_name = cmd.split("table")[1].split("(")[0].strip()
                print(f"    [OK] Created/merged table: {table_name}")
            elif ".create-or-alter" in cmd:
                print(f"    [OK] Created ingestion mapping")
            elif ".alter" in cmd:
                print(f"    [OK] Applied policy")
            success_count += 1

    print(f"  [OK] Executed {success_count}/{len(KQL_TABLE_COMMANDS)} KQL commands")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 70)
    print("  SETUP REAL-TIME INTELLIGENCE RESOURCES")
    print("=" * 70)
    print(f"  Workspace      : {WORKSPACE_NAME}")
    print(f"  Eventhouse     : {EVENTHOUSE_NAME}")
    print(f"  KQL Database   : {KQL_DB_NAME}")
    print()

    # -- Step 1: Authenticate --
    print("Step 1: Authenticating...")
    token = get_fabric_token(TENANT_ID, ADMIN_ACCOUNT)
    print()

    # -- Step 2: Get workspace --
    print("Step 2: Finding workspace...")
    workspace_id = get_workspace_id(token)
    print(f"  [OK] Workspace ID: {workspace_id}")
    print()

    # -- Step 3: Create Eventhouse --
    print("Step 3: Creating Eventhouse...")
    eventhouse_id = create_eventhouse(token, workspace_id)
    if not eventhouse_id:
        print("  [FAIL] Could not create Eventhouse. Exiting.")
        sys.exit(1)
    print()

    # -- Step 4: Create KQL Database --
    print("Step 4: Creating KQL Database...")
    kql_db_id = create_kql_database(token, workspace_id, eventhouse_id)
    if not kql_db_id:
        print("  [FAIL] Could not create KQL Database. Exiting.")
        sys.exit(1)
    print()

    # -- Step 5: Get connection string --
    print("Step 5: Getting KQL connection string...")
    cluster_uri = get_kql_connection_string(token, workspace_id, kql_db_id)
    if cluster_uri:
        print(f"  [OK] Cluster URI: {cluster_uri}")
    else:
        print("  [WARN] Could not get cluster URI. Tables will be created manually.")
    print()

    # -- Step 6: Create tables --
    if cluster_uri:
        print("Step 6: Creating KQL tables and mappings...")
        setup_kql_tables(token, cluster_uri, KQL_DB_NAME)
    else:
        print("Step 6: SKIPPED (no cluster URI)")
        print("  Create tables manually in KQL Database:")
        for cmd in KQL_TABLE_COMMANDS[:2]:
            print(f"    {cmd.strip()[:80]}...")
    print()

    # -- Summary --
    print("=" * 70)
    print("  SETUP SUMMARY")
    print("=" * 70)
    print(f"  Eventhouse:    {EVENTHOUSE_NAME} ({eventhouse_id})")
    print(f"  KQL Database:  {KQL_DB_NAME} ({kql_db_id})")
    print(f"  Cluster URI:   {cluster_uri or '[manual]'}")
    print()
    print("  KQL Tables Created:")
    print("    • bikerentaldb — station availability events")
    print("    • weather_tb   — weather observations")
    print()
    print("=" * 70)
    print("  NEXT STEPS: Create Eventstream in Fabric UI")
    print("=" * 70)
    print()
    print("  1. Go to Fabric Workspace → + New → Eventstream")
    print(f"  2. Name: {EVENTSTREAM_NAME}")
    print()
    print("  3. Add SOURCE:")
    print("     Option A: Custom App (for the simulator notebook)")
    print("       - Source type: Custom App")
    print("       - Copy the Connection String for the simulator")
    print()
    print("     Option B: API (for external feeds)")
    print("       - Source type: Azure Event Hub or REST API")
    print()
    print("  4. Add DESTINATION:")
    print(f"     - Destination type: KQL Database")
    print(f"     - Database: {KQL_DB_NAME}")
    print(f"     - Table: bikerentaldb")
    print("     - Input format: JSON")
    print()
    print("  5. For weather data, add second destination:")
    print(f"     - Table: weather_tb")
    print()
    print("  6. Run the Event Simulator notebook to start streaming:")
    print("     - NB10_Event_Simulator_Stream.Notebook")
    print()
    print("  7. Deploy the RTI Dashboard:")
    print("     - python 04_deploy_rti_dashboard.py")
    print()


if __name__ == "__main__":
    main()
