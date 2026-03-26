"""
06_deploy_data_agent.py  –  Deploy Bicycle Fleet Intelligence Agent to Fabric
=============================================================================

Creates (or updates) a Data Agent in the target Fabric workspace using the
REST API.  The agent definition is read from:

    data_agents/Bicycle_Fleet_Intelligence_Agent/

The script automatically:
  • Resolves the target workspace by name
  • Loads all definition parts listed in manifest.json
  • Base64-encodes each part for the API payload
  • Auto-maps data sources (bicycles_gold lakehouse + Bicycle RTI Analytics
    semantic model) to real Fabric item IDs in the target workspace
  • Creates a "Data Agents" folder (if it doesn't exist)
  • Creates or updates the agent via the Fabric DataAgents REST API

Usage:
    python 06_deploy_data_agent.py              # Create new agent
    python 06_deploy_data_agent.py --update      # Update existing agent
    python 06_deploy_data_agent.py --skip-patching  # Deploy without ID patching

Requirements:
    pip install azure-identity requests
"""

import argparse
import base64
import json
import requests
import sys
import time
from pathlib import Path

from fabric_auth import get_fabric_token, get_auth_headers
from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

# ============================================================
# CONSTANTS
# ============================================================

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
AGENT_NAME = "Bicycle Fleet Intelligence Agent"
TARGET_WORKSPACE_NAME = WORKSPACE_NAME          # from .env
ACCOUNT = ADMIN_ACCOUNT                          # from .env

# Agent definition lives next to this script
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_AGENTS_DIR = SCRIPT_DIR / "data_agents"


# ============================================================
# WORKSPACE HELPERS
# ============================================================

def get_workspace_id(token: str, workspace_name: str) -> str:
    """Resolve workspace name → workspace ID."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    for ws in resp.json().get("value", []):
        if ws["displayName"].lower() == workspace_name.lower():
            return ws["id"]

    print(f"  [FAIL] Workspace '{workspace_name}' not found")
    sys.exit(1)


def get_workspace_items(token: str, workspace_id: str) -> list:
    """List all items in a workspace (paginated)."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
    items = []

    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        items.extend(data.get("value", []))
        url = data.get("continuationUri")

    return items


def find_item_by_name(items: list, name: str) -> dict | None:
    """Find first item matching the display name (case-insensitive)."""
    for item in items:
        if item["displayName"].lower() == name.lower():
            return item
    return None


def find_item_by_name_and_type(items: list, name: str,
                               fabric_type: str) -> dict | None:
    """Find an item matching both name and Fabric type."""
    for item in items:
        if (item["displayName"].lower() == name.lower()
                and item.get("type", "").lower() == fabric_type.lower()):
            return item
    return None


# ============================================================
# FOLDER MANAGEMENT
# ============================================================

def get_or_create_folder(token: str, workspace_id: str,
                         folder_name: str) -> str | None:
    """Get or create a workspace folder. Returns folder ID or None."""
    headers = get_auth_headers(token)

    # Check existing items for the folder
    items = get_workspace_items(token, workspace_id)
    for item in items:
        if (item.get("type") == "Folder"
                and item["displayName"].lower() == folder_name.lower()):
            print(f"  [OK] Folder exists: {folder_name} ({item['id']})")
            return item["id"]

    # Create the folder
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
    payload = {
        "displayName": folder_name,
        "type": "Folder",
    }
    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code in (200, 201):
        folder_id = resp.json().get("id")
        print(f"  [OK] Created folder: {folder_name} ({folder_id})")
        return folder_id
    elif resp.status_code == 409:
        print(f"  [OK] Folder already exists: {folder_name}")
        # Re-fetch to get the ID
        items = get_workspace_items(token, workspace_id)
        for item in items:
            if (item.get("type") == "Folder"
                    and item["displayName"].lower() == folder_name.lower()):
                return item["id"]
        return None
    else:
        print(f"  [WARN] Could not create folder: HTTP {resp.status_code}")
        print(f"    {resp.text[:300]}")
        return None


# ============================================================
# LONG-RUNNING OPERATION HELPER
# ============================================================

def wait_for_operation(token: str, response, label: str,
                       max_wait: int = 120) -> bool:
    """Poll a Fabric long-running operation until completion."""
    headers = get_auth_headers(token)
    operation_url = response.headers.get("Location")
    retry_after = int(response.headers.get("Retry-After", 5))

    if not operation_url:
        print(f"  [WARN] No Location header for LRO: {label}")
        return False

    elapsed = 0
    while elapsed < max_wait:
        time.sleep(retry_after)
        elapsed += retry_after

        resp = requests.get(operation_url, headers=headers)
        if resp.status_code != 200:
            print(f"  [WARN] LRO poll returned HTTP {resp.status_code}")
            continue

        status = resp.json().get("status", "").lower()
        if status == "succeeded":
            return True
        elif status in ("failed", "cancelled"):
            print(f"  [FAIL] Operation {status}: {label}")
            error = resp.json().get("error", {})
            if error:
                print(f"    {json.dumps(error, indent=2)[:500]}")
            return False

        # Still running — loop
        retry_after = int(resp.headers.get("Retry-After", retry_after))

    print(f"  [FAIL] Timeout waiting for operation: {label}")
    return False


# ============================================================
# DATA AGENT HELPERS
# ============================================================

def list_existing_agents(token: str, workspace_id: str) -> list:
    """List all DataAgent items in the workspace."""
    items = get_workspace_items(token, workspace_id)
    return [i for i in items if i.get("type") == "DataAgent"]


# Map datasource type → Fabric item type
_DS_TYPE_TO_FABRIC_TYPE = {
    "lakehouse_tables": "Lakehouse",
    "semantic_model":   "SemanticModel",
    "warehouse":        "Warehouse",
    "kql_database":     "KQLDatabase",
}


def build_auto_mapping(token: str, workspace_id: str,
                       parts: list) -> dict:
    """
    Scan datasource.json files in the definition parts to discover which
    data sources the agent needs.  Then look them up in the target workspace
    to build a name → {id, workspaceId} mapping.

    Returns: { "<display_name>": {"id": "...", "workspaceId": "..."} }
    """
    # Collect display names + types from datasource.json parts
    needed: dict[str, str] = {}   # displayName → fabric type

    for part in parts:
        if not part["path"].endswith("datasource.json"):
            continue
        try:
            raw = base64.b64decode(part["payload"]).decode("utf-8")
            ds = json.loads(raw)
            ds_type = ds.get("type", "")
            ds_name = ds.get("displayName", "")
            fabric_type = _DS_TYPE_TO_FABRIC_TYPE.get(ds_type)
            if ds_name and fabric_type:
                needed[ds_name] = fabric_type
                print(f"    Source: {ds_name} ({fabric_type})")
        except Exception:
            continue

    if not needed:
        print("  [WARN] No data sources found in definition parts")
        return {}

    # Look up each source in the target workspace
    ws_items = get_workspace_items(token, workspace_id)
    mapping = {}

    for name, fabric_type in needed.items():
        item = find_item_by_name_and_type(ws_items, name, fabric_type)
        if item:
            mapping[name] = {
                "id": item["id"],
                "workspaceId": item.get("workspaceId", workspace_id),
            }
            print(f"    Mapped: {name} → {item['id']}")
        else:
            print(f"    [WARN] Not found in workspace: {name} ({fabric_type})")

    return mapping


def patch_data_sources(parts: list, workspace_id: str,
                       auto_mapping: dict) -> list:
    """
    Patch artifactId and workspaceId in every datasource.json part so that
    the agent binds to the correct items in the target workspace.
    """
    patched_parts = []

    for part in parts:
        part_path = part["path"]
        payload = part["payload"]
        payload_type = part.get("payloadType", "InlineBase64")

        if part_path.endswith("datasource.json") and auto_mapping:
            try:
                raw = base64.b64decode(payload).decode("utf-8")
                ds = json.loads(raw)
                ds_name = ds.get("displayName", "")

                if ds_name in auto_mapping:
                    target = auto_mapping[ds_name]
                    old_id = ds.get("artifactId", "")
                    ds["artifactId"] = target["id"]
                    ds["workspaceId"] = target.get("workspaceId",
                                                    workspace_id)
                    patched = json.dumps(ds, indent=2, ensure_ascii=False)
                    payload = base64.b64encode(
                        patched.encode("utf-8")
                    ).decode("utf-8")
                    print(f"    Patched: {ds_name}")
                    print(f"      {old_id} → {target['id']}")
            except Exception as exc:
                print(f"    [WARN] Patch error on {part_path}: {exc}")

        patched_parts.append({
            "path": part_path,
            "payload": payload,
            "payloadType": payload_type,
        })

    return patched_parts


# ============================================================
# LOAD & DEPLOY
# ============================================================

def load_agent_parts(agent_dir: Path) -> list:
    """
    Load definition parts from disk using the manifest.json.
    Re-encodes each file as base64 for the API.
    """
    manifest_path = agent_dir / "manifest.json"
    if not manifest_path.exists():
        print(f"  [FAIL] No manifest.json found in {agent_dir}")
        sys.exit(1)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    parts = []

    for part_info in manifest.get("exportedParts", []):
        part_path = part_info["path"]
        file_path = agent_dir / part_path

        if not file_path.exists():
            print(f"  [WARN] Missing file: {file_path}")
            continue

        raw = file_path.read_bytes()
        payload_b64 = base64.b64encode(raw).decode("utf-8")

        parts.append({
            "path": part_path,
            "payload": payload_b64,
            "payloadType": "InlineBase64",
        })
        print(f"    {part_path} ({len(raw):,} bytes)")

    return parts


def create_data_agent(token: str, workspace_id: str, name: str,
                      description: str, parts: list,
                      folder_id: str = None) -> str | None:
    """Create a new data agent via the Fabric DataAgents API."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/DataAgents"

    payload = {
        "displayName": name,
        "description": description,
        "definition": {"parts": parts},
    }
    if folder_id:
        payload["folderId"] = folder_id

    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code in (200, 201):
        item_id = resp.json().get("id")
        print(f"  [OK] Created: {name} (ID: {item_id})")
        return item_id
    elif resp.status_code == 202:
        success = wait_for_operation(token, resp, name)
        if success:
            agents = list_existing_agents(token, workspace_id)
            item = find_item_by_name(agents, name)
            if item:
                print(f"  [OK] Created: {name} (ID: {item['id']})")
                return item["id"]
        print(f"  [FAIL] Failed to create: {name}")
        return None
    elif resp.status_code == 409 or "AlreadyInUse" in resp.text:
        print(f"  [WARN] Already exists: {name}")
        return "exists"
    else:
        print(f"  [FAIL] Create failed: HTTP {resp.status_code}")
        print(f"    {resp.text[:500]}")
        return None


def update_data_agent(token: str, workspace_id: str, agent_id: str,
                      name: str, parts: list) -> bool:
    """Update an existing data agent definition."""
    headers = get_auth_headers(token)
    url = (f"{FABRIC_API_BASE}/workspaces/{workspace_id}"
           f"/DataAgents/{agent_id}/updateDefinition")

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
        description="Deploy Bicycle Fleet Intelligence Agent to Fabric"
    )
    parser.add_argument(
        "--workspace", default=TARGET_WORKSPACE_NAME,
        help=f"Target workspace (default: {TARGET_WORKSPACE_NAME})",
    )
    parser.add_argument(
        "--agent", default=AGENT_NAME,
        help=f"Data Agent name (default: {AGENT_NAME})",
    )
    parser.add_argument(
        "--update", action="store_true",
        help="Update an existing data agent definition",
    )
    parser.add_argument(
        "--skip-patching", action="store_true",
        help="Skip data source patching (use IDs as-is from export)",
    )
    parser.add_argument(
        "--folder", default="Data Agents",
        help="Workspace folder name (default: Data Agents)",
    )
    args = parser.parse_args()

    # Derive folder name from agent display name
    agent_dir = DATA_AGENTS_DIR / args.agent.replace(" ", "_")

    print("=" * 70)
    print("  DEPLOY DATA AGENT TO FABRIC")
    print("=" * 70)
    print(f"  Target Workspace: {args.workspace}")
    print(f"  Agent Name:       {args.agent}")
    print(f"  Source:           {agent_dir}")
    print(f"  Mode:             {'Update existing' if args.update else 'Create new'}")
    print()

    if not agent_dir.exists():
        print(f"  [FAIL] Agent directory not found: {agent_dir}")
        print(f"  Ensure data_agents/{args.agent.replace(' ', '_')}/ exists.")
        sys.exit(1)

    # -- Step 1: Authenticate --
    print("Step 1: Authenticating...")
    token = get_fabric_token(TENANT_ID, ACCOUNT)
    print()

    # -- Step 2: Resolve workspace --
    print("Step 2: Finding workspace...")
    workspace_id = get_workspace_id(token, args.workspace)
    print(f"  [OK] Workspace ID: {workspace_id}")
    print()

    # -- Step 3: Load definition parts from disk --
    print("Step 3: Loading agent definition from disk...")
    parts = load_agent_parts(agent_dir)
    if not parts:
        print("  [FAIL] No definition parts loaded")
        sys.exit(1)
    print(f"  [OK] Loaded {len(parts)} part(s)")
    print()

    # -- Step 4-5: Auto-map data sources to target workspace --
    if not args.skip_patching:
        print("Step 4: Auto-mapping data sources to target workspace...")
        auto_mapping = build_auto_mapping(token, workspace_id, parts)
        print()

        print("Step 5: Patching data source bindings...")
        parts = patch_data_sources(parts, workspace_id, auto_mapping)
        print(f"  [OK] Patching complete")
    else:
        print("Step 4-5: Skipped data source patching (--skip-patching)")
    print()

    # -- Step 6: Create/find folder --
    print("Step 6: Creating workspace folder...")
    folder_id = get_or_create_folder(token, workspace_id, args.folder)
    print()

    # -- Step 7: Check existing agents --
    print("Step 7: Checking existing data agents...")
    existing_agents = list_existing_agents(token, workspace_id)
    existing = find_item_by_name(existing_agents, args.agent)
    print(f"  Found {len(existing_agents)} data agent(s) in workspace")
    if existing:
        print(f"  Existing match: {existing['id']}")
    print()

    # -- Step 8: Deploy --
    description = (
        "Bicycle Fleet Intelligence Agent — AI-powered data agent for "
        "bike-share fleet analytics over the gold lakehouse and semantic "
        "model. Covers station availability, demand forecasting, "
        "rebalancing operations, weather impact, and neighbourhood insights."
    )
    print("Step 8: Deploying...")
    if existing and args.update:
        print(f"  Updating: {args.agent}...")
        ok = update_data_agent(
            token, workspace_id, existing["id"], args.agent, parts
        )
        result = "[OK] Updated" if ok else "[FAIL] Update failed"
    elif existing and not args.update:
        print(f"  [WARN] Already exists: {args.agent} (use --update)")
        result = "[WARN] Already exists"
    else:
        print(f"  Creating: {args.agent}...")
        item_id = create_data_agent(
            token, workspace_id, args.agent, description, parts, folder_id
        )
        result = "[OK] Created" if item_id else "[FAIL] Failed"

    print()
    print("=" * 70)
    print("  DEPLOYMENT SUMMARY")
    print("=" * 70)
    print(f"  {args.agent:<50} {result}")
    print()
    print("  Post-deployment checklist:")
    print("    1. Open the agent in Fabric portal")
    print("    2. Verify data source connections are active")
    print("    3. Check AI instructions in the stage config")
    print("    4. Test a few queries against the 35 few-shot examples")
    print("    5. Publish the agent if it deployed in draft state")
    print()

    if "FAIL" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
