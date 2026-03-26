"""
Deploy Real-Time Dashboard to Microsoft Fabric
===============================================
Creates a Real-Time Dashboard in the Fabric workspace and pushes
the dashboard definition (pages, tiles, KQL queries, data sources).

The dashboard definition lives in:
    rti_dashboard/dashboard.json

Dashboard layout (2 pages, 8 tiles):
  Page 1 — Fleet Operations (real-time)
    • Fleet Summary KPIs       (stat card)
    • Live Station Map         (map — lat/lon colored by status)
    • Rebalancing Queue        (table — critical dispatch list)
    • Availability Trend 6h    (line chart — 5-min resolution)

  Page 2 — Intelligence & SLA
    • Neighbourhood Demand     (bar chart — utilisation by zone)
    • SLA Compliance           (table — station uptime tracking)
    • Anomaly Detection        (table — z-score outliers)
    • Dynamic Pricing Signals  (table — supply/demand pricing)

Usage:
    python 04_deploy_rti_dashboard.py
    python 04_deploy_rti_dashboard.py --update
"""

import sys
import json
import base64
import time
import argparse
import requests
from pathlib import Path

from fabric_auth import get_fabric_token, get_auth_headers
from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ============================================================
# CONFIGURATION
# ============================================================

DASHBOARD_NAME = "Bicycle Fleet Operations"
EVENTHOUSE_NAME = "bikerentaleventhouse"
KQL_DATABASE_NAME = "bikerentaldb"

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
DASHBOARD_DIR = Path(__file__).parent / "rti_dashboard"


# ============================================================
# API HELPERS
# ============================================================

def get_workspace_id(token: str, workspace_name: str) -> str:
    url = f"{FABRIC_API_BASE}/workspaces"
    resp = requests.get(url, headers=get_auth_headers(token))
    resp.raise_for_status()
    for ws in resp.json().get("value", []):
        if ws["displayName"] == workspace_name:
            return ws["id"]
    available = [w["displayName"] for w in resp.json().get("value", [])]
    print(f"  [FAIL] Workspace '{workspace_name}' not found")
    print(f"    Available: {available}")
    sys.exit(1)


def get_eventhouse_info(token: str, workspace_id: str):
    """
    Get the Eventhouse ID and its query URI.
    Returns (eventhouse_id, query_uri) or (None, None).
    """
    headers = get_auth_headers(token)

    # List Eventhouses
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=Eventhouse"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"  [WARN] Could not list Eventhouses: HTTP {resp.status_code}")
        return None, None

    eventhouse_id = None
    for item in resp.json().get("value", []):
        if item["displayName"] == EVENTHOUSE_NAME:
            eventhouse_id = item["id"]
            break

    if not eventhouse_id:
        print(f"  [WARN] Eventhouse '{EVENTHOUSE_NAME}' not found")
        return None, None

    # Get Eventhouse properties to find query URI
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/eventhouses/{eventhouse_id}"
    resp = requests.get(url, headers=headers)
    query_uri = None
    if resp.status_code == 200:
        props = resp.json().get("properties", {})
        query_uri = props.get("queryServiceUri")
        if not query_uri:
            # Also check ingestionServiceUri or databasesItemIds
            query_uri = props.get("uri")

    if not query_uri:
        # Fallback: construct from workspace region
        # Fabric Eventhouse URIs follow: https://{guid}.{region}.kusto.fabric.microsoft.com
        print("  [INFO] queryServiceUri not directly available, will try item properties")
        # Try getting the KQL database for connection info
        kql_url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=KQLDatabase"
        kql_resp = requests.get(kql_url, headers=headers)
        if kql_resp.status_code == 200:
            for kql_item in kql_resp.json().get("value", []):
                if kql_item["displayName"] == KQL_DATABASE_NAME:
                    # Try to get query URI from KQL database properties
                    kql_detail_url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/kqlDatabases/{kql_item['id']}"
                    kql_detail = requests.get(kql_detail_url, headers=headers)
                    if kql_detail.status_code == 200:
                        kql_props = kql_detail.json().get("properties", {})
                        query_uri = kql_props.get("queryUri") or kql_props.get("parentEventhouseUri")
                    break

    return eventhouse_id, query_uri


def get_kql_database_id(token: str, workspace_id: str):
    """Get the KQL database ID."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=KQLDatabase"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        for item in resp.json().get("value", []):
            if item["displayName"] == KQL_DATABASE_NAME:
                return item["id"]
    return None


def wait_for_operation(token: str, response, item_name: str,
                       timeout_seconds: int = 180) -> bool:
    headers = get_auth_headers(token)
    operation_url = response.headers.get("Location")
    if not operation_url:
        time.sleep(10)
        return True

    print(f"    ... Provisioning {item_name}...")
    start = time.time()
    while time.time() - start < timeout_seconds:
        retry_after = int(response.headers.get("Retry-After", 5))
        time.sleep(retry_after)
        op_resp = requests.get(operation_url, headers=headers)
        if op_resp.status_code == 200:
            status = op_resp.json().get("status", "")
            if status == "Succeeded":
                return True
            elif status in ("Failed", "Cancelled"):
                error = op_resp.json().get("error", {}).get("message", "")
                print(f"    [FAIL] {item_name}: {status} -- {error}")
                return False
        elif op_resp.status_code == 202:
            continue
        elif op_resp.status_code == 404:
            time.sleep(3)
            return True
    print(f"    [FAIL] {item_name} timed out ({timeout_seconds}s)")
    return False


# ============================================================
# DASHBOARD DEFINITION BUILDER
# ============================================================

def build_dashboard_definition(workspace_id: str,
                                eventhouse_id: str,
                                query_uri: str) -> list:
    """
    Load dashboard.json, patch in the Eventhouse connection info,
    and return the definition parts (base64-encoded).
    """
    dashboard_json_path = DASHBOARD_DIR / "dashboard.json"
    if not dashboard_json_path.exists():
        print(f"  [FAIL] Definition not found: {dashboard_json_path}")
        sys.exit(1)

    raw = dashboard_json_path.read_text(encoding="utf-8")

    # Patch placeholders
    if query_uri:
        raw = raw.replace("__EVENTHOUSE_QUERY_URI__", query_uri)
    if eventhouse_id:
        raw = raw.replace("__EVENTHOUSE_ID__", eventhouse_id)

    # Validate JSON
    try:
        dashboard_def = json.loads(raw)
        print(f"  [OK] Dashboard definition loaded ({len(dashboard_def.get('pages', []))} pages, "
              f"{len(dashboard_def.get('queries', []))} queries)")
    except json.JSONDecodeError as e:
        print(f"  [FAIL] Invalid JSON in dashboard definition: {e}")
        sys.exit(1)

    # Fabric Items API expects definition parts
    payload_b64 = base64.b64encode(raw.encode("utf-8")).decode("utf-8")

    parts = [
        {
            "path": "RealTimeDashboard.json",
            "payload": payload_b64,
            "payloadType": "InlineBase64",
        }
    ]

    return parts


# ============================================================
# CREATE / UPDATE
# ============================================================

def list_dashboards(token: str, workspace_id: str) -> list:
    """List Real-Time Dashboards in the workspace."""
    headers = get_auth_headers(token)
    # Try known RTI dashboard type names
    for item_type in ["RealTimeDashboard", "KQLDashboard", "Dashboard"]:
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type={item_type}"
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            items = resp.json().get("value", [])
            if items:  # Found dashboards with this type
                return items, item_type
    # No dashboards found with any type — return preferred create type
    return [], "RealTimeDashboard"


def find_item_by_name(items: list, name: str):
    for item in items:
        if item.get("displayName") == name:
            return item
    return None


def create_dashboard(token: str, workspace_id: str, name: str,
                     parts: list, item_type: str) -> str:
    """Create a new Real-Time Dashboard."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"

    payload = {
        "displayName": name,
        "type": item_type,
        "definition": {"parts": parts},
    }

    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code in (200, 201):
        item_id = resp.json().get("id")
        print(f"  [OK] Created: {name} (ID: {item_id})")
        return item_id
    elif resp.status_code == 202:
        success = wait_for_operation(token, resp, name)
        if success:
            dashboards, _ = list_dashboards(token, workspace_id)
            item = find_item_by_name(dashboards, name)
            if item:
                print(f"  [OK] Created: {name} (ID: {item['id']})")
                return item["id"]
        print(f"  [FAIL] Failed to create: {name}")
        return None
    elif resp.status_code == 409:
        print(f"  [WARN] Already exists: {name} (use --update to overwrite)")
        return "exists"
    else:
        print(f"  [FAIL] Create failed: HTTP {resp.status_code}")
        details = resp.text[:600]
        print(f"    {details}")

        # If item type was wrong, try alternate types
        if "InvalidItemType" in details or "unsupported" in details.lower() or "not supported" in details.lower():
            alternate_types = ["RealTimeDashboard", "KQLDashboard", "Dashboard"]
            alternate_types = [t for t in alternate_types if t != item_type]
            for alt_type in alternate_types:
                print(f"\n  [RETRY] Trying item type '{alt_type}'...")
                payload["type"] = alt_type
                resp2 = requests.post(url, headers=headers, json=payload)
                if resp2.status_code in (200, 201):
                    item_id = resp2.json().get("id")
                    print(f"  [OK] Created with type '{alt_type}': {name} (ID: {item_id})")
                    return item_id
                elif resp2.status_code == 202:
                    success = wait_for_operation(token, resp2, name)
                    if success:
                        dashboards, _ = list_dashboards(token, workspace_id)
                        item = find_item_by_name(dashboards, name)
                        if item:
                            print(f"  [OK] Created: {name} (ID: {item['id']})")
                            return item["id"]
                else:
                    print(f"    {alt_type}: HTTP {resp2.status_code} — {resp2.text[:200]}")

        return None


def update_dashboard(token: str, workspace_id: str, item_id: str,
                     name: str, parts: list) -> bool:
    """Update an existing dashboard definition."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{item_id}/updateDefinition"

    payload = {"definition": {"parts": parts}}
    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code in (200, 201):
        print(f"  [OK] Updated: {name}")
        return True
    elif resp.status_code == 202:
        success = wait_for_operation(token, resp, name)
        if success:
            print(f"  [OK] Updated: {name}")
        else:
            print(f"  [FAIL] Update failed: {name}")
        return success
    else:
        print(f"  [FAIL] Update failed: HTTP {resp.status_code}")
        print(f"    {resp.text[:500]}")
        return False


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Deploy Bicycle Fleet RTI Dashboard to Fabric"
    )
    parser.add_argument(
        "--workspace", default=WORKSPACE_NAME,
        help=f"Target workspace (default: {WORKSPACE_NAME})",
    )
    parser.add_argument(
        "--name", default=DASHBOARD_NAME,
        help=f"Dashboard name (default: {DASHBOARD_NAME})",
    )
    parser.add_argument(
        "--update", action="store_true",
        help="Update existing dashboard definition",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("  DEPLOY REAL-TIME DASHBOARD TO FABRIC")
    print("=" * 70)
    print(f"  Target Workspace : {args.workspace}")
    print(f"  Dashboard Name   : {args.name}")
    print(f"  Eventhouse       : {EVENTHOUSE_NAME}")
    print(f"  KQL Database     : {KQL_DATABASE_NAME}")
    print(f"  Mode             : {'Update' if args.update else 'Create'}")
    print()

    # -- Step 1: Authenticate --
    print("Step 1: Authenticating...")
    token = get_fabric_token(TENANT_ID, ADMIN_ACCOUNT)
    print()

    # -- Step 2: Resolve workspace --
    print("Step 2: Finding workspace...")
    workspace_id = get_workspace_id(token, args.workspace)
    print(f"  [OK] Workspace ID: {workspace_id}")
    print()

    # -- Step 3: Get Eventhouse connection info --
    print("Step 3: Resolving Eventhouse connection...")
    eventhouse_id, query_uri = get_eventhouse_info(token, workspace_id)
    if eventhouse_id:
        print(f"  [OK] Eventhouse ID : {eventhouse_id}")
    if query_uri:
        print(f"  [OK] Query URI     : {query_uri}")
    else:
        print("  [WARN] Could not resolve query URI automatically")
        print("         Dashboard will be created but may need manual data source connection")
    print()

    # -- Step 4: Build definition --
    print("Step 4: Building dashboard definition...")
    parts = build_dashboard_definition(workspace_id, eventhouse_id, query_uri)
    print()

    # -- Step 5: Check existing dashboards --
    print("Step 5: Checking existing dashboards...")
    existing_dashboards, detected_type = list_dashboards(token, workspace_id)
    existing = find_item_by_name(existing_dashboards, args.name)
    print(f"  Found {len(existing_dashboards)} dashboard(s) in workspace")
    print(f"  Item type: {detected_type}")
    if existing:
        print(f"  [INFO] Dashboard '{args.name}' already exists (ID: {existing['id']})")
    print()

    # -- Step 6: Deploy --
    print("Step 6: Deploying...")
    if existing and args.update:
        ok = update_dashboard(token, workspace_id, existing["id"], args.name, parts)
        result = "[OK] Updated" if ok else "[FAIL] Update failed"
    elif existing and not args.update:
        print(f"  [WARN] Already exists: {args.name} (use --update to overwrite)")
        result = "[WARN] Already exists"
    else:
        item_id = create_dashboard(token, workspace_id, args.name, parts, detected_type)
        result = "[OK] Created" if item_id and item_id != "exists" else "[FAIL] Failed"

    print()
    print("=" * 70)
    print("  DEPLOYMENT SUMMARY")
    print("=" * 70)
    print(f"  {args.name:<45} {result}")
    print()

    # -- Dashboard layout summary --
    print("  Dashboard Layout:")
    print("  ┌──────────────────────────────────────────────────────────┐")
    print("  │ PAGE 1: Fleet Operations                                │")
    print("  │   ┌─────────────────┬──────────────────────────────┐   │")
    print("  │   │ Fleet Summary   │ Live Station Map             │   │")
    print("  │   │ (Stat KPIs)     │ (Map — lat/lon by status)    │   │")
    print("  │   ├─────────────────┤                              │   │")
    print("  │   │ Rebalancing     │                              │   │")
    print("  │   │ Queue (Table)   │                              │   │")
    print("  │   ├─────────────────┴──────────────────────────────┤   │")
    print("  │   │ Availability Trend — 6h Line Chart (5-min)     │   │")
    print("  │   └────────────────────────────────────────────────┘   │")
    print("  │                                                        │")
    print("  │ PAGE 2: Intelligence & SLA                              │")
    print("  │   ┌─────────────────┬──────────────────────────────┐   │")
    print("  │   │ Neighbourhood   │ SLA Compliance               │   │")
    print("  │   │ Demand (Bar)    │ (Table)                      │   │")
    print("  │   ├─────────────────┼──────────────────────────────┤   │")
    print("  │   │ Anomaly Detect. │ Dynamic Pricing              │   │")
    print("  │   │ (Table)         │ (Table)                      │   │")
    print("  │   └─────────────────┴──────────────────────────────┘   │")
    print("  └──────────────────────────────────────────────────────────┘")
    print()

    if "FAIL" in result:
        print("  FALLBACK: Create the dashboard manually in Fabric UI:")
        print("    1. Workspace → + New → Real-Time Dashboard")
        print(f"    2. Name it: {args.name}")
        print(f"    3. Connect data source: {EVENTHOUSE_NAME} → {KQL_DATABASE_NAME}")
        print("    4. Add tiles using the KQL queries from NB05")
        print("    5. (KQL queries are also saved in rti_dashboard/dashboard.json)")
        sys.exit(1)


if __name__ == "__main__":
    main()
