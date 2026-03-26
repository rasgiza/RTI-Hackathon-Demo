"""
Deploy Ontology to Microsoft Fabric
=====================================
Reads the exported Ontology definition from post_deploy/definitions/ and
deploys it to a target Fabric workspace using the Ontology REST API.

The definition consists of:
  - definition.json                     Root ontology definition
  - EntityTypes/{id}/definition.json    12 entity types with properties
  - EntityTypes/{id}/DataBindings/*.json Data bindings to lakehouse tables
  - RelationshipTypes/{id}/definition.json           23 relationship types
  - RelationshipTypes/{id}/Contextualizations/*.json Edge table bindings

All data bindings point to the bicycles_gold lakehouse. The script
automatically patches workspace/lakehouse GUIDs to match the target
environment.

API reference:
  POST /workspaces/{id}/ontologies                         — create
  POST /workspaces/{id}/ontologies/{id}/updateDefinition   — push definition
  POST /workspaces/{id}/ontologies/{id}/getDefinition      — read definition

Usage:
    python deploy_ontology.py
    python deploy_ontology.py --update
    python deploy_ontology.py --workspace "My Workspace"
"""

import sys
import os
import json
import base64
import re
import argparse
import requests
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fabric_auth import get_fabric_token, get_auth_headers
from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME
from clients.ontology_client import OntologyClient

# ============================================================
# CONFIGURATION
# ============================================================
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
ONTOLOGY_NAME = "Bicycle_Ontology_Model_New"
LAKEHOUSE_NAME = "bicycles_gold"
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
ONTOLOGY_DIR = PROJECT_DIR / "post_deploy" / "definitions" / f"{ONTOLOGY_NAME}.Ontology"


# ============================================================
# WORKSPACE HELPERS
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


def get_lakehouse_id(token: str, ws_id: str, name: str) -> str | None:
    """Resolve lakehouse ID by display name."""
    url = f"{FABRIC_API_BASE}/workspaces/{ws_id}/lakehouses"
    resp = requests.get(url, headers=get_auth_headers(token))
    if resp.status_code != 200:
        return None
    for lh in resp.json().get("value", []):
        if lh["displayName"] == name:
            return lh["id"]
    return None


# ============================================================
# LOAD ONTOLOGY FROM DISK
# ============================================================

def load_ontology_parts(ontology_dir: Path) -> list[dict]:
    """
    Walk the exported ontology directory and build base64-encoded parts.

    Expected structure:
      definition.json
      EntityTypes/{id}/definition.json
      EntityTypes/{id}/DataBindings/{id}.json
      RelationshipTypes/{id}/definition.json
      RelationshipTypes/{id}/Contextualizations/{id}.json
    """
    parts = []
    for root, _dirs, files in os.walk(ontology_dir):
        for fname in sorted(files):
            if fname == ".platform":
                continue  # Skip fabric-cicd metadata
            filepath = Path(root) / fname
            rel_path = filepath.relative_to(ontology_dir).as_posix()

            raw = filepath.read_bytes()
            # Strip BOM if present
            if raw.startswith(b'\xef\xbb\xbf'):
                raw = raw[3:]

            payload_b64 = base64.b64encode(raw).decode("utf-8")
            parts.append({
                "path": rel_path,
                "payload": payload_b64,
                "payloadType": "InlineBase64",
            })

    return parts


# ============================================================
# DATA BINDING PATCHING
# ============================================================

def patch_data_bindings(parts: list[dict], workspace_id: str, lakehouse_id: str) -> list[dict]:
    """
    Replace workspace/lakehouse IDs and OneLake paths in data binding
    and contextualization parts so the ontology points to the correct
    target environment.
    """
    patched = []
    for part in parts:
        path = part.get("path", "")
        payload = part.get("payload", "")
        payload_type = part.get("payloadType", "InlineBase64")

        needs_patching = (
            "DataBindings" in path or
            "Contextualizations" in path
        )

        if needs_patching and payload_type == "InlineBase64" and payload:
            try:
                content = base64.b64decode(payload).decode("utf-8")
                content = _patch_binding_json(content, workspace_id, lakehouse_id)
                payload = base64.b64encode(content.encode("utf-8")).decode("utf-8")
            except Exception as e:
                print(f"    [WARN] Could not patch {path}: {e}")

        patched.append({
            "path": path,
            "payload": payload,
            "payloadType": payload_type,
        })
    return patched


def _patch_binding_json(content: str, workspace_id: str, lakehouse_id: str) -> str:
    """Patch workspace/lakehouse IDs in a JSON binding string."""
    try:
        obj = json.loads(content)
        _patch_binding_obj(obj, workspace_id, lakehouse_id)
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        return content


def _patch_binding_obj(obj, workspace_id: str, lakehouse_id: str):
    """Recursively patch workspace/lakehouse IDs in a dict or list."""
    if isinstance(obj, dict):
        for key in list(obj.keys()):
            val = obj[key]
            lower_key = key.lower()
            if lower_key in ("workspaceid", "workspaceguid", "workspace_id"):
                obj[key] = workspace_id
            elif lower_key in ("itemid", "lakehouseid", "artifactid",
                               "lakehouse_id", "artifact_id", "item_id"):
                obj[key] = lakehouse_id
            elif isinstance(val, str) and "onelake" in val.lower():
                patched = _patch_onelake_path(val, workspace_id, lakehouse_id)
                if patched != val:
                    obj[key] = patched
            elif isinstance(val, (dict, list)):
                _patch_binding_obj(val, workspace_id, lakehouse_id)
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                _patch_binding_obj(item, workspace_id, lakehouse_id)


def _patch_onelake_path(path: str, workspace_id: str, lakehouse_id: str) -> str:
    """Patch OneLake abfss:// or https:// paths."""
    m = re.match(
        r'(abfss://)([0-9a-f-]+)(@onelake[^/]*/)([0-9a-f-]+)(.*)',
        path, re.IGNORECASE,
    )
    if m:
        return f"{m.group(1)}{workspace_id}{m.group(3)}{lakehouse_id}{m.group(5)}"
    m = re.match(
        r'(https://onelake[^/]+/)([0-9a-f-]+)/([0-9a-f-]+)(.*)',
        path, re.IGNORECASE,
    )
    if m:
        return f"{m.group(1)}{workspace_id}/{lakehouse_id}{m.group(4)}"
    return path


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Deploy Ontology to Fabric")
    parser.add_argument("--workspace", default=WORKSPACE_NAME,
                        help=f"Target workspace (default: {WORKSPACE_NAME})")
    parser.add_argument("--ontology", default=ONTOLOGY_NAME,
                        help=f"Ontology display name (default: {ONTOLOGY_NAME})")
    parser.add_argument("--update", action="store_true",
                        help="Update existing ontology definition")
    parser.add_argument("--skip-patching", action="store_true",
                        help="Skip data binding patching")
    args = parser.parse_args()

    print("=" * 70)
    print("  DEPLOY ONTOLOGY TO FABRIC")
    print("=" * 70)
    print(f"  Target Workspace: {args.workspace}")
    print(f"  Ontology Name:    {args.ontology}")
    print(f"  Source:           {ONTOLOGY_DIR}")
    print(f"  Lakehouse:        {LAKEHOUSE_NAME}")
    print(f"  Mode:             {'Update existing' if args.update else 'Create new'}")
    print()

    if not ONTOLOGY_DIR.exists():
        print(f"  [FAIL] Ontology directory not found: {ONTOLOGY_DIR}")
        sys.exit(1)

    # Step 1: Authenticate
    print("Step 1: Authenticating...")
    token = get_fabric_token(TENANT_ID, ADMIN_ACCOUNT)
    client = OntologyClient(token)
    print()

    # Step 2: Resolve workspace
    print("Step 2: Finding workspace...")
    ws_id = get_workspace_id(token, args.workspace)
    print(f"  [OK] Workspace ID: {ws_id}")
    print()

    # Step 3: Resolve lakehouse
    print("Step 3: Finding target lakehouse...")
    lh_id = get_lakehouse_id(token, ws_id, LAKEHOUSE_NAME)
    if lh_id:
        print(f"  [OK] {LAKEHOUSE_NAME}: {lh_id}")
    else:
        print(f"  [WARN] {LAKEHOUSE_NAME} not found — data bindings may fail")
        print(f"         Deploy workspace items first (Deploy_Bicycle_RTI.ipynb)")
        lh_id = "PLACEHOLDER-LAKEHOUSE-ID"
    print()

    # Step 4: Load definition from disk
    print("Step 4: Loading ontology definition from disk...")
    parts = load_ontology_parts(ONTOLOGY_DIR)
    if not parts:
        print("  [FAIL] No definition parts loaded")
        sys.exit(1)

    # Count items
    entity_count = sum(1 for p in parts
                       if p["path"].startswith("EntityTypes/")
                       and p["path"].endswith("/definition.json"))
    binding_count = sum(1 for p in parts if "DataBindings/" in p["path"])
    rel_count = sum(1 for p in parts
                    if p["path"].startswith("RelationshipTypes/")
                    and p["path"].endswith("/definition.json"))
    ctx_count = sum(1 for p in parts if "Contextualizations/" in p["path"])

    print(f"  [OK] Loaded {len(parts)} parts:")
    print(f"       {entity_count} entity types")
    print(f"       {binding_count} data bindings")
    print(f"       {rel_count} relationship types")
    print(f"       {ctx_count} contextualizations")
    print()

    # Step 5: Patch data bindings
    if not args.skip_patching:
        print("Step 5: Patching data bindings for target environment...")
        print(f"  Target workspace: {ws_id}")
        print(f"  Target lakehouse: {lh_id}")
        parts = patch_data_bindings(parts, ws_id, lh_id)
        print(f"  [OK] Patching complete")
    else:
        print("Step 5: Skipped data binding patching (--skip-patching)")
    print()

    # Step 6: Deploy
    result = ""
    existing = client.find_by_name(ws_id, args.ontology)
    print(f"Step 6: {'Updating' if existing and args.update else 'Creating'} ontology...")

    if existing and args.update:
        print(f"  Updating: {args.ontology} (ID: {existing['id']})")
        ok = client.update_definition(ws_id, existing["id"], parts)
        result = "[OK] Definition updated" if ok else "[FAIL] Update failed"

    elif existing and not args.update:
        print(f"  [WARN] Already exists: {args.ontology} (ID: {existing['id']})")
        print(f"  Use --update to overwrite the definition")
        result = "[SKIP] Already exists"

    else:
        description = (
            f"Bicycle Fleet Ontology — {entity_count} entity types, "
            f"{rel_count} relationships, bound to {LAKEHOUSE_NAME} lakehouse."
        )
        print(f"  Creating: {args.ontology}...")
        ont_id = client.create(ws_id, args.ontology, description)
        if ont_id:
            print(f"  Pushing full definition ({len(parts)} parts)...")
            ok = client.update_definition(ws_id, ont_id, parts)
            result = ("[OK] Created + definition applied" if ok
                      else "[WARN] Created but definition update failed")
        else:
            result = "[FAIL] Create failed"
    print()

    # Step 7: Verify
    print("Step 7: Verifying...")
    verified = client.find_by_name(ws_id, args.ontology)
    if verified:
        print(f"  [OK] Ontology confirmed: {verified['displayName']} ({verified['id']})")

        # Optionally read back the definition to verify
        decoded = client.get_definition_decoded(ws_id, verified["id"])
        if decoded:
            et_count = sum(1 for k in decoded if k.startswith("EntityTypes/")
                          and k.endswith("/definition.json"))
            rt_count = sum(1 for k in decoded if k.startswith("RelationshipTypes/")
                          and k.endswith("/definition.json"))
            db_count = sum(1 for k in decoded if "DataBindings/" in k)
            print(f"  Verified definition: {et_count} entities, {rt_count} relationships, {db_count} bindings")
    else:
        print(f"  [WARN] Could not verify ontology")
    print()

    # Summary
    print("=" * 70)
    print("  DEPLOYMENT SUMMARY")
    print("=" * 70)
    print(f"  {args.ontology:<45} {result}")
    print()
    print("  " + "-" * 66)
    print("  NEXT STEPS")
    print("  " + "-" * 66)
    print("  1. Open the ontology in Fabric UI to verify entity types")
    print("  2. The graph model (Bicycle_Ontology_Model_New_graph) is")
    print("     auto-provisioned by the ontology when you open it")
    print("  3. Click 'Refresh now' in the graph model to load data")
    print("     (API refresh is NOT supported for ontology-managed graphs)")
    print("  4. Or deploy a standalone graph: python deploy_graph_model.py")
    print()

    if "FAIL" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
