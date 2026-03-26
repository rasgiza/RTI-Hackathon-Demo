"""
Setup Fabric Resources -- Bicycle RTI Demo Provisioning
========================================================
Creates ALL Fabric artifacts needed for the Real-Time Intelligence demo:
  - 3 Lakehouses : bikerental_bronze_raw (existing), bicycles_silver, bicycles_gold
  - 1 Eventhouse  : bikerentaleventhouse  (KQL DB for real-time queries)

The bikerental_bronze_raw lakehouse already exists in the workspace with the
Fabric sample bicycle data (bikeraw_tb).

Usage:
    python 01_setup_fabric_resources.py
"""

import requests
import sys
import time
from fabric_auth import get_fabric_token, get_auth_headers
from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ============================================================
# CONFIGURATION
# ============================================================
ACCOUNT = ADMIN_ACCOUNT
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

# Lakehouses to create (Bronze already exists from sample data)
LAKEHOUSES = [
    {
        "name": "bicycles_silver",
        "description": "Silver layer -- cleaned, validated, enriched bicycle station data",
        "enable_schemas": False,
    },
    {
        "name": "bicycles_gold",
        "description": "Gold layer -- star schema (dims + facts) for bicycle analytics",
        "enable_schemas": False,
    },
]

# Eventhouse for real-time ingestion via Eventstream
EVENTHOUSE_NAME = "bikerentaleventhouse"
EVENTHOUSE_DESCRIPTION = "Real-Time Intelligence hub -- KQL database for streaming bicycle telemetry"

# KQL Database inside the Eventhouse
KQL_DB_NAME = "bicycles_kqldb"
KQL_DB_DESCRIPTION = "KQL database for real-time bicycle availability queries, anomaly detection, dashboards"

# Workspace folders
NOTEBOOK_FOLDER_NAME = "Notebooks"
RTI_FOLDER_NAME = "Real-Time Intelligence"


# ============================================================
# API HELPERS
# ============================================================

def get_or_create_workspace(token: str, workspace_name: str) -> str:
    """Find existing workspace by name or create it. Returns workspace ID."""
    url = f"{FABRIC_API_BASE}/workspaces"
    headers = get_auth_headers(token)

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"  [FAIL] Failed to list workspaces: {response.status_code}")
        sys.exit(1)

    for ws in response.json().get("value", []):
        if ws["displayName"] == workspace_name:
            print(f"  [OK] Found existing workspace: {workspace_name}")
            return ws["id"]

    # Create workspace
    print(f"  Workspace '{workspace_name}' not found -- creating...")
    create_resp = requests.post(
        url,
        headers=headers,
        json={"displayName": workspace_name},
    )
    if create_resp.status_code in (200, 201):
        ws_id = create_resp.json()["id"]
        print(f"  [OK] Created workspace: {workspace_name} (ID: {ws_id})")
        return ws_id
    else:
        print(f"  [FAIL] Failed to create workspace: {create_resp.status_code}")
        print(f"    {create_resp.text[:300]}")
        sys.exit(1)


def get_item_id(token: str, workspace_id: str, item_type: str, item_name: str) -> str | None:
    """Get item ID by type and name. Returns None if not found."""
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/{item_type}"
    headers = get_auth_headers(token)
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return None
    for item in response.json().get("value", []):
        if item["displayName"] == item_name:
            return item["id"]
    return None


def get_or_create_folder(token: str, workspace_id: str, folder_name: str) -> str | None:
    """Get or create a workspace folder. Returns folder ID."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/folders"

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        for folder in response.json().get("value", []):
            if folder["displayName"] == folder_name:
                print(f"  [OK] Folder '{folder_name}' exists (ID: {folder['id']})")
                return folder["id"]

    payload = {"displayName": folder_name}
    print(f"  Creating folder '{folder_name}'...")
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code in (200, 201):
        folder_id = response.json()["id"]
        print(f"  [OK] Folder '{folder_name}' created (ID: {folder_id})")
        return folder_id
    elif response.status_code == 409 or "FolderDisplayNameAlreadyInUse" in response.text:
        print(f"  [OK] Folder '{folder_name}' already exists (conflict)")
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
            time.sleep(3)
            return True
    print(f"    [FAIL] {item_name} timed out ({timeout_seconds}s)")
    return False


# ============================================================
# CREATE LAKEHOUSE
# ============================================================

def create_lakehouse(
    token: str, workspace_id: str, name: str, description: str,
    enable_schemas: bool = False, folder_id: str | None = None,
) -> str | None:
    """Create a lakehouse. Returns lakehouse ID."""
    existing_id = get_item_id(token, workspace_id, "lakehouses", name)
    if existing_id:
        print(f"  [OK] {name} already exists (ID: {existing_id})")
        return existing_id

    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
    headers = get_auth_headers(token)

    payload = {
        "type": "Lakehouse",
        "displayName": name,
        "description": description,
    }
    if folder_id:
        payload["folderId"] = folder_id
    if enable_schemas:
        payload["creationPayload"] = {"enableSchemas": True}

    schema_label = "schema-enabled" if enable_schemas else "schema-less"
    print(f"  Creating {name} ({schema_label})...")
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code in (200, 201):
        lakehouse_id = response.json()["id"]
        print(f"  [OK] {name} created (ID: {lakehouse_id})")
        return lakehouse_id
    elif response.status_code == 202:
        success = wait_for_long_running_operation(token, response, name)
        if success:
            new_id = get_item_id(token, workspace_id, "lakehouses", name)
            if new_id:
                print(f"  [OK] {name} created (ID: {new_id})")
                return new_id
        return None
    elif response.status_code == 409:
        print(f"  [OK] {name} already exists (conflict)")
        return get_item_id(token, workspace_id, "lakehouses", name)
    else:
        print(f"  [FAIL] {name} -- HTTP {response.status_code}")
        print(f"    {response.text[:300]}")
        return None


# ============================================================
# CREATE EVENTHOUSE + KQL DATABASE
# ============================================================

def create_eventhouse(token: str, workspace_id: str, name: str, description: str) -> str | None:
    """Create an Eventhouse. Returns eventhouse ID."""
    # Check if exists
    existing_id = get_item_id(token, workspace_id, "eventhouses", name)
    if existing_id:
        print(f"  [OK] Eventhouse '{name}' already exists (ID: {existing_id})")
        return existing_id

    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
    headers = get_auth_headers(token)

    payload = {
        "type": "Eventhouse",
        "displayName": name,
        "description": description,
    }

    print(f"  Creating Eventhouse '{name}'...")
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code in (200, 201):
        eh_id = response.json()["id"]
        print(f"  [OK] Eventhouse '{name}' created (ID: {eh_id})")
        return eh_id
    elif response.status_code == 202:
        success = wait_for_long_running_operation(token, response, name, timeout_seconds=180)
        if success:
            new_id = get_item_id(token, workspace_id, "eventhouses", name)
            if new_id:
                print(f"  [OK] Eventhouse '{name}' created (ID: {new_id})")
                return new_id
        return None
    elif response.status_code == 409:
        print(f"  [OK] Eventhouse '{name}' already exists (conflict)")
        return get_item_id(token, workspace_id, "eventhouses", name)
    else:
        print(f"  [FAIL] Eventhouse '{name}' -- HTTP {response.status_code}")
        print(f"    {response.text[:300]}")
        return None


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 70)
    print("BICYCLE RTI -- FABRIC RESOURCE SETUP")
    print("=" * 70)
    print(f"  Tenant:    {TENANT_ID}")
    print(f"  Account:   {ACCOUNT}")
    print(f"  Workspace: {WORKSPACE_NAME}")
    print()

    # Authenticate
    print("Step 1: Authenticating...")
    token = get_fabric_token(TENANT_ID, ACCOUNT)
    print()

    # Get workspace
    print("Step 2: Finding workspace...")
    workspace_id = get_or_create_workspace(token, WORKSPACE_NAME)
    print()

    # Check Bronze lakehouse (should exist from sample data)
    print("Step 3: Verifying bikerental_bronze_raw lakehouse...")
    bronze_id = get_item_id(token, workspace_id, "lakehouses", "bikerental_bronze_raw")
    if bronze_id:
        print(f"  [OK] bikerental_bronze_raw exists (ID: {bronze_id})")
    else:
        print("  [WARN] bikerental_bronze_raw NOT found!")
        print("    -> Load the Fabric sample bicycle dataset first")
        print("    -> Or create it manually: Lakehouse -> New -> bikerental_bronze_raw")
    print()

    # Create Silver + Gold lakehouses
    print("Step 4: Creating Silver & Gold lakehouses...")
    lakehouse_ids = {}
    for lh in LAKEHOUSES:
        lh_id = create_lakehouse(
            token, workspace_id,
            name=lh["name"],
            description=lh["description"],
            enable_schemas=lh["enable_schemas"],
        )
        if lh_id:
            lakehouse_ids[lh["name"]] = lh_id
    print()

    # Create Eventhouse for real-time intelligence
    print("Step 5: Creating Eventhouse...")
    eh_id = create_eventhouse(
        token, workspace_id,
        name=EVENTHOUSE_NAME,
        description=EVENTHOUSE_DESCRIPTION,
    )
    print()

    # Create workspace folders
    print("Step 6: Creating workspace folders...")
    nb_folder_id = get_or_create_folder(token, workspace_id, NOTEBOOK_FOLDER_NAME)
    rti_folder_id = get_or_create_folder(token, workspace_id, RTI_FOLDER_NAME)
    print()

    # Summary
    print("=" * 70)
    print("SETUP SUMMARY")
    print("=" * 70)
    print(f"  Workspace:         {WORKSPACE_NAME} ({workspace_id})")
    print(f"  Bronze Lakehouse:  {'[OK]' if bronze_id else '[MISSING]'} bikerental_bronze_raw")
    for lh in LAKEHOUSES:
        status = "[OK]" if lh["name"] in lakehouse_ids else "[FAIL]"
        print(f"  {lh['name']:<20} {status}")
    print(f"  Eventhouse:        {'[OK]' if eh_id else '[FAIL]'} {EVENTHOUSE_NAME}")
    print(f"  Notebook Folder:   {'[OK]' if nb_folder_id else '[FAIL]'}")
    print(f"  RTI Folder:        {'[OK]' if rti_folder_id else '[FAIL]'}")
    print()
    print("NEXT STEPS:")
    print("  1. Create Eventstream in Fabric UI:")
    print("     -> Name: bicycles_eventstream")
    print("     -> Source: Custom App (for simulator) or Eventhouse")
    print("  2. Deploy notebooks:")
    print("     -> python 02_deploy_notebooks.py")
    print("  3. Run Event Simulator notebook to start streaming")
    print()


if __name__ == "__main__":
    main()
