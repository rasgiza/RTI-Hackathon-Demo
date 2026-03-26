"""
Deploy Power BI Report to Microsoft Fabric
============================================
Reads the Bicycle Fleet Report PBIR definition from disk and deploys it
to the target Fabric workspace using the Items REST API.

The report definition lives in:
    reports/Bicycle_Fleet_Report/
        definition.pbir   – binds the report to the semantic model
        report.json       – full PBIR visual layout (3 pages, 19 visuals)

Key features:
  - Auto-resolves the semantic model ID by name in the target workspace
  - Patches definition.pbir so the report binds to the correct model
  - Creates a "Reports" workspace folder for organisation
  - Supports --update to overwrite an existing report

Usage:
    python 05_deploy_report.py
    python 05_deploy_report.py --update
    python 05_deploy_report.py --workspace "My Other Workspace"
"""

import sys
import os
import json
import base64
import time
import argparse
import requests
from pathlib import Path

from fabric_auth import get_fabric_token, get_auth_headers
from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

# Ensure stdout handles Unicode on Windows
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ============================================================
# CONFIGURATION
# ============================================================

ACCOUNT = ADMIN_ACCOUNT
TARGET_WORKSPACE_NAME = WORKSPACE_NAME

REPORT_NAME = "Bicycle Fleet Operations Report"
SEMANTIC_MODEL_NAME = "Bicycle RTI Analytics"
REPORT_FOLDER_NAME = "Reports"

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

# Where PBIR report definitions live on disk
REPORTS_DIR = Path(__file__).parent / "reports"
REPORT_SUBDIR = "Bicycle_Fleet_Report"


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


def get_semantic_model_id(token: str, workspace_id: str,
                          model_name: str) -> str | None:
    """Look up a semantic model by name in the workspace."""
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=SemanticModel"
    resp = requests.get(url, headers=get_auth_headers(token))
    if resp.status_code != 200:
        print(f"  [WARN] Could not list semantic models: HTTP {resp.status_code}")
        return None
    for item in resp.json().get("value", []):
        if item["displayName"] == model_name:
            return item["id"]
    return None


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


def list_existing_reports(token: str, workspace_id: str) -> list:
    """List all reports in the workspace."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=Report"
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
# LOAD / PATCH / DEPLOY
# ============================================================

def load_report_parts(report_dir: Path, semantic_model_id: str) -> list:
    """
    Load PBIR report definition from disk and base64-encode for the API.

    Files expected:
      - definition.pbir  (connection binding to semantic model)
      - report.json      (visual layout, pages, queries)

    The definition.pbir is patched to point at the resolved semantic model ID.
    """
    parts = []

    # --- definition.pbir ---
    pbir_path = report_dir / "definition.pbir"
    if not pbir_path.exists():
        print(f"  [FAIL] definition.pbir not found in {report_dir}")
        sys.exit(1)

    pbir_data = json.loads(pbir_path.read_text(encoding="utf-8"))
    # Patch the semantic model reference
    old_id = pbir_data.get("datasetReference", {}).get("byConnection", {}).get(
        "pbiModelDatabaseName", "")
    if semantic_model_id and old_id != semantic_model_id:
        pbir_data["datasetReference"]["byConnection"]["pbiModelDatabaseName"] = semantic_model_id
        print(f"  [PATCH] definition.pbir: model ID {old_id[:12]}... → {semantic_model_id[:12]}...")
    else:
        print(f"  [OK] definition.pbir already points to correct model")

    pbir_bytes = json.dumps(pbir_data, indent=2).encode("utf-8")
    parts.append({
        "path": "definition.pbir",
        "payload": base64.b64encode(pbir_bytes).decode("utf-8"),
        "payloadType": "InlineBase64",
    })
    print(f"    definition.pbir ({len(pbir_bytes):,} bytes)")

    # --- report.json ---
    report_path = report_dir / "report.json"
    if not report_path.exists():
        print(f"  [FAIL] report.json not found in {report_dir}")
        sys.exit(1)

    report_bytes = report_path.read_bytes()
    parts.append({
        "path": "report.json",
        "payload": base64.b64encode(report_bytes).decode("utf-8"),
        "payloadType": "InlineBase64",
    })
    print(f"    report.json ({len(report_bytes):,} bytes)")

    return parts


def create_report(token: str, workspace_id: str, name: str,
                  parts: list, folder_id: str = None):
    """Create a new Power BI Report via Fabric Items API. Returns item ID."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"

    payload = {
        "displayName": name,
        "type": "Report",
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
            reports = list_existing_reports(token, workspace_id)
            item = find_item_by_name(reports, name)
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
        try:
            err = resp.json()
            print(f"    Error: {err.get('error', {}).get('code', 'N/A')}")
            print(f"    Message: {err.get('error', {}).get('message', resp.text[:500])}")
        except Exception:
            print(f"    {resp.text[:500]}")
        return None


def update_report(token: str, workspace_id: str, item_id: str,
                  name: str, parts: list) -> bool:
    """Update an existing report definition."""
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
        try:
            err = resp.json()
            print(f"    Error: {err.get('error', {}).get('code', 'N/A')}")
            print(f"    Message: {err.get('error', {}).get('message', resp.text[:500])}")
        except Exception:
            print(f"    {resp.text[:500]}")
        return False


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Deploy Bicycle Fleet Power BI Report to Fabric"
    )
    parser.add_argument(
        "--workspace", default=TARGET_WORKSPACE_NAME,
        help=f"Target workspace (default: {TARGET_WORKSPACE_NAME})",
    )
    parser.add_argument(
        "--report", default=REPORT_NAME,
        help=f"Report name (default: {REPORT_NAME})",
    )
    parser.add_argument(
        "--model", default=SEMANTIC_MODEL_NAME,
        help=f"Semantic model to bind to (default: {SEMANTIC_MODEL_NAME})",
    )
    parser.add_argument(
        "--update", action="store_true",
        help="Update existing report definition instead of creating new",
    )
    args = parser.parse_args()

    report_dir = REPORTS_DIR / REPORT_SUBDIR

    print("=" * 70)
    print("  DEPLOY POWER BI REPORT TO FABRIC")
    print("=" * 70)
    print(f"  Target Workspace : {args.workspace}")
    print(f"  Report Name      : {args.report}")
    print(f"  Semantic Model   : {args.model}")
    print(f"  Source Directory  : {report_dir}")
    print(f"  Mode             : {'Update existing' if args.update else 'Create new'}")
    print()

    if not report_dir.exists():
        print(f"  [FAIL] Report directory not found: {report_dir}")
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

    # -- Step 3: Resolve semantic model --
    print("Step 3: Finding semantic model...")
    model_id = get_semantic_model_id(token, workspace_id, args.model)
    if model_id:
        print(f"  [OK] Semantic model '{args.model}' → {model_id}")
    else:
        print(f"  [WARN] Semantic model '{args.model}' not found in workspace.")
        print(f"    The report may not bind to data correctly until the model is deployed.")
        print(f"    Deploy it first with: python 03_deploy_semantic_model.py")
        model_id = None
    print()

    # -- Step 4: Load and patch definition --
    print("Step 4: Loading report definition from disk...")
    parts = load_report_parts(report_dir, model_id)
    if not parts:
        print("  [FAIL] No definition parts loaded")
        sys.exit(1)
    print(f"  [OK] Loaded {len(parts)} part(s)")
    print()

    # -- Step 5: Create workspace folder --
    print("Step 5: Creating workspace folder...")
    folder_id = get_or_create_folder(token, workspace_id, REPORT_FOLDER_NAME)
    print()

    # -- Step 6: Check existing reports --
    print("Step 6: Checking existing reports...")
    existing_reports = list_existing_reports(token, workspace_id)
    existing = find_item_by_name(existing_reports, args.report)
    print(f"  Found {len(existing_reports)} report(s) in workspace")
    if existing:
        print(f"  [INFO] Report '{args.report}' already exists (ID: {existing['id']})")
    print()

    # -- Step 7: Deploy --
    print("Step 7: Deploying...")
    if existing and args.update:
        print(f"  Updating: {args.report}...")
        ok = update_report(token, workspace_id, existing["id"], args.report, parts)
        result = "[OK] Updated" if ok else "[FAIL] Update failed"
    elif existing and not args.update:
        print(f"  [WARN] Already exists: {args.report} (use --update to overwrite)")
        result = "[WARN] Already exists"
    else:
        print(f"  Creating: {args.report}...")
        item_id = create_report(token, workspace_id, args.report, parts, folder_id)
        result = "[OK] Created" if item_id and item_id != "exists" else "[FAIL] Failed"

    print()
    print("=" * 70)
    print("  DEPLOYMENT SUMMARY")
    print("=" * 70)
    print(f"  {args.report:<45} {result}")
    print()

    if result.startswith("[OK]"):
        print("  Next steps:")
        print(f"    1. Open Fabric workspace → {args.workspace}")
        print(f"    2. Find '{args.report}' in the Reports folder")
        print(f"    3. The report auto-connects to '{args.model}' (Direct Lake)")
        print(f"    4. All 3 pages render visuals from the lakehouse Gold tables")
        print()

    if "FAIL" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
