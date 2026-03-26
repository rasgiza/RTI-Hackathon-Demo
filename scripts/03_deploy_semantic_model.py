"""
Deploy Semantic Model to Microsoft Fabric
==========================================
Reads the Bicycle RTI Analytics semantic-model definition (TMDL) from disk
and deploys it to the target Fabric workspace using the Items REST API.

The model definition lives in:
    semantic_models/Bicycle_RTI_Analytics/

Key features:
  - Reads definition.json manifest → base64-encodes each file part
  - Auto-patches expressions.tmdl OneLake GUIDs to target workspace/lakehouse
  - Creates "Semantic Models" workspace folder for organisation
  - Supports --update to overwrite an existing model

Usage:
    python 03_deploy_semantic_model.py
    python 03_deploy_semantic_model.py --update
    python 03_deploy_semantic_model.py --model "Bicycle RTI Analytics"
    python 03_deploy_semantic_model.py --workspace "Healthcare-Analytics-Dev"
"""

import sys
import os
import json
import base64
import re
import time
import argparse
import requests
from pathlib import Path

from fabric_auth import get_fabric_token, get_auth_headers
from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

# Ensure stdout handles any stray Unicode on Windows cmd.exe / older PowerShell
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ============================================================
# CONFIGURATION
# ============================================================

ACCOUNT = ADMIN_ACCOUNT
TARGET_WORKSPACE_NAME = WORKSPACE_NAME

SEMANTIC_MODEL_NAME = "Bicycle RTI Analytics"
LAKEHOUSE_NAME_FOR_MODEL = "bicycles_gold"
SEMANTIC_MODEL_FOLDER_NAME = "Semantic Models"

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

# Where TMDL model definitions live on disk
MODELS_DIR = Path(__file__).parent / "semantic_models"

# Regex to match OneLake URLs with GUID pairs (or PLACEHOLDER tokens)
ONELAKE_URL_PATTERN = re.compile(
    r"https://onelake\.dfs\.fabric\.microsoft\.com/"
    r"[\w-]+"   # workspace id or placeholder
    r"/"
    r"[\w-]+"   # lakehouse id or placeholder
)


# ============================================================
# API HELPERS
# ============================================================

def get_workspace_id(token: str, workspace_name: str) -> str:
    """Look up workspace ID by display name."""
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


def get_or_create_folder(token: str, workspace_id: str, folder_name: str):
    """Get or create a workspace folder. Returns folder ID or None."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/folders"

    # Check existing folders
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        for folder in response.json().get("value", []):
            if folder["displayName"] == folder_name:
                print(f"  [OK] Folder '{folder_name}' already exists (ID: {folder['id']})")
                return folder["id"]

    # Create new folder
    payload = {"displayName": folder_name}
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code in (200, 201):
        folder_id = response.json()["id"]
        print(f"  [OK] Folder '{folder_name}' created (ID: {folder_id})")
        return folder_id
    elif response.status_code == 409 or "FolderDisplayNameAlreadyInUse" in response.text:
        print(f"  [OK] Folder '{folder_name}' already exists (conflict)")
        resp2 = requests.get(url, headers=headers)
        if resp2.status_code == 200:
            for folder in resp2.json().get("value", []):
                if folder["displayName"] == folder_name:
                    return folder["id"]
        return None
    else:
        print(f"  [WARN] Could not create folder: {response.status_code}")
        return None


def get_lakehouse_id(token: str, workspace_id: str, lakehouse_name: str) -> str | None:
    """Look up a lakehouse by name in the target workspace."""
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=Lakehouse"
    resp = requests.get(url, headers=get_auth_headers(token))
    if resp.status_code != 200:
        print(f"  [WARN] Could not list lakehouses: HTTP {resp.status_code}")
        return None
    for item in resp.json().get("value", []):
        if item["displayName"] == lakehouse_name:
            return item["id"]
    return None


def list_existing_models(token: str, workspace_id: str) -> list:
    """List all semantic models in the workspace."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=SemanticModel"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json().get("value", [])
    return []


def find_item_by_name(items: list, name: str):
    """Find an item in a list by displayName."""
    for item in items:
        if item.get("displayName") == name:
            return item
    return None


def wait_for_operation(token: str, response, item_name: str,
                       timeout_seconds: int = 180) -> bool:
    """Poll a long-running operation until it succeeds, fails, or times out."""
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
# GUID PATCHING -- expressions.tmdl
# ============================================================
# The on-disk expressions.tmdl contains PLACEHOLDER tokens for the
# OneLake connection URL:
#
#   "https://onelake.dfs.fabric.microsoft.com/
#       WORKSPACE_ID_PLACEHOLDER/LAKEHOUSE_ID_PLACEHOLDER"
#
# At deploy time we replace these with the actual workspace and
# lakehouse IDs of bicycles_gold in the target workspace.
# ============================================================

def patch_model_connection(parts: list, target_workspace_id: str,
                           target_lakehouse_id: str) -> int:
    """
    In-place patch of expressions.tmdl in the parts list.
    Replaces OneLake URL placeholders with actual workspace/lakehouse GUIDs.
    Returns the number of replacements made.
    """
    new_url = (
        f"https://onelake.dfs.fabric.microsoft.com/"
        f"{target_workspace_id}/{target_lakehouse_id}"
    )
    count = 0
    for part in parts:
        if "expressions.tmdl" in part["path"]:
            raw = base64.b64decode(part["payload"]).decode("utf-8")
            updated, n = ONELAKE_URL_PATTERN.subn(new_url, raw)
            if n > 0:
                part["payload"] = base64.b64encode(
                    updated.encode("utf-8")
                ).decode("utf-8")
                count += n
    return count


# ============================================================
# LOAD / CREATE / UPDATE
# ============================================================

def load_model_parts(model_dir: Path) -> list:
    """
    Load definition parts from disk.
    Reads the definition.json manifest and base64-encodes each referenced file.
    """
    manifest_path = model_dir / "definition.json"
    if not manifest_path.exists():
        print(f"  [FAIL] No definition.json found in {model_dir}")
        sys.exit(1)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    parts = []

    for part_info in manifest.get("exportedParts", []):
        part_path = part_info["path"]
        file_path = model_dir / part_path

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


def create_semantic_model(token: str, workspace_id: str, name: str,
                          parts: list, folder_id: str = None):
    """Create a new semantic model. Returns item ID or None."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"

    payload = {
        "displayName": name,
        "type": "SemanticModel",
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
            models = list_existing_models(token, workspace_id)
            item = find_item_by_name(models, name)
            if item:
                print(f"  [OK] Created: {name} (ID: {item['id']})")
                return item["id"]
        print(f"  [FAIL] Failed to create: {name}")
        return None
    elif resp.status_code == 409:
        print(f"  [WARN] Already exists: {name}")
        return "exists"
    else:
        print(f"  [FAIL] Create failed: HTTP {resp.status_code}")
        print(f"    {resp.text[:500]}")
        return None


def update_semantic_model(token: str, workspace_id: str, item_id: str,
                          name: str, parts: list) -> bool:
    """Update an existing semantic model definition."""
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
        description="Deploy Bicycle RTI Analytics Semantic Model to Fabric"
    )
    parser.add_argument(
        "--workspace", default=TARGET_WORKSPACE_NAME,
        help=f"Target workspace (default: {TARGET_WORKSPACE_NAME})",
    )
    parser.add_argument(
        "--model", default=SEMANTIC_MODEL_NAME,
        help=f"Model name (default: {SEMANTIC_MODEL_NAME})",
    )
    parser.add_argument(
        "--update", action="store_true",
        help="Update existing model definition instead of creating new",
    )
    args = parser.parse_args()

    # Derive the on-disk folder name (spaces -> underscores)
    model_folder = MODELS_DIR / args.model.replace(" ", "_")

    print("=" * 70)
    print("  DEPLOY SEMANTIC MODEL TO FABRIC")
    print("=" * 70)
    print(f"  Target Workspace : {args.workspace}")
    print(f"  Model Name       : {args.model}")
    print(f"  Source Directory  : {model_folder}")
    print(f"  Target Lakehouse : {LAKEHOUSE_NAME_FOR_MODEL}")
    print(f"  Mode             : {'Update existing' if args.update else 'Create new'}")
    print()

    if not model_folder.exists():
        print(f"  [FAIL] Model directory not found: {model_folder}")
        print(f"    Expected: semantic_models/{args.model.replace(' ', '_')}/")
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
    print("Step 3: Loading model definition from disk...")
    parts = load_model_parts(model_folder)
    if not parts:
        print("  [FAIL] No definition parts loaded")
        sys.exit(1)
    print(f"  [OK] Loaded {len(parts)} part(s)")
    print()

    # -- Step 3b: Patch Direct Lake connection GUIDs --
    print("Step 3b: Patching Direct Lake connection to target workspace...")
    lh_id = get_lakehouse_id(token, workspace_id, LAKEHOUSE_NAME_FOR_MODEL)
    if lh_id:
        n = patch_model_connection(parts, workspace_id, lh_id)
        if n:
            print(f"  [OK] Patched {n} OneLake URL(s) -> workspace={workspace_id}, lakehouse={lh_id}")
        else:
            print("  [INFO] No OneLake URLs found to patch (may already be correct)")
    else:
        print(f"  [WARN] Lakehouse '{LAKEHOUSE_NAME_FOR_MODEL}' not found in workspace --")
        print(f"    Skipping connection patching. The model may not bind to data correctly.")
        print(f"    Ensure '{LAKEHOUSE_NAME_FOR_MODEL}' lakehouse exists before opening the model.")
    print()

    # -- Step 4: Create workspace folder --
    print("Step 4: Creating workspace folder...")
    folder_id = get_or_create_folder(token, workspace_id, SEMANTIC_MODEL_FOLDER_NAME)
    print()

    # -- Step 5: Check existing models --
    print("Step 5: Checking existing models...")
    existing_models = list_existing_models(token, workspace_id)
    existing = find_item_by_name(existing_models, args.model)
    print(f"  Found {len(existing_models)} semantic model(s) in workspace")
    if existing:
        print(f"  [INFO] Model '{args.model}' already exists (ID: {existing['id']})")
    print()

    # -- Step 6: Deploy --
    print("Step 6: Deploying...")
    if existing and args.update:
        print(f"  Updating: {args.model}...")
        ok = update_semantic_model(token, workspace_id, existing["id"], args.model, parts)
        result = "[OK] Updated" if ok else "[FAIL] Update failed"
    elif existing and not args.update:
        print(f"  [WARN] Already exists: {args.model} (use --update to overwrite)")
        result = "[WARN] Already exists"
    else:
        print(f"  Creating: {args.model}...")
        item_id = create_semantic_model(token, workspace_id, args.model, parts, folder_id)
        result = "[OK] Created" if item_id else "[FAIL] Failed"

    print()
    print("=" * 70)
    print("  DEPLOYMENT SUMMARY")
    print("=" * 70)
    print(f"  {args.model:<45} {result}")
    print()

    if "FAIL" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
