#!/usr/bin/env python3
"""
Deploy GraphModel from Ontology Metadata
==========================================
Reads ontology entity types, relationships, data bindings, and
contextualizations from the local ontology folder, then deploys a standalone
GraphModel item via the Fabric GraphModel REST API.

Uses the reusable client wrappers:
  - clients.graph_client.GraphModelClient          — REST API wrapper
  - clients.graph_definition_builder.GraphDefinitionBuilder — 5-part builder

Modes:
  Default   — Create a new graph model (or skip if it already exists).
  --update  — Push a fresh definition to an existing graph model.
  --fix-existing — Fix an auto-provisioned graph model's entity keys,
              contextualizations, and edge discovery by regenerating the
              definition from ontology metadata and pushing via updateDefinition.
              Use this after creating an ontology in the Fabric UI, which
              auto-provisions a graph model with default (incorrect) schema.

Usage:
    python deploy_graph_model.py
    python deploy_graph_model.py --graph-model "My_Graph"
    python deploy_graph_model.py --update
    python deploy_graph_model.py --fix-existing

API reference:
  POST /workspaces/{id}/graphModels                          — create
  POST /workspaces/{id}/graphModels/{id}/updateDefinition    — fix schema
  POST /workspaces/{id}/graphModels/{id}/getDefinition       — inspect
  GET  /workspaces/{id}/graphModels/{id}/queryableGraphType  — preview schema
  POST /workspaces/{id}/graphModels/{id}/executeQuery        — GQL query

Ported from HLS project: Healthcare-Data-Analytics-Repo/FabricDemoHLS/05b_deploy_graph_model.py
"""

import sys
import json
import argparse
import requests
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fabric_auth import get_fabric_token, get_auth_headers
from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME
from clients.graph_client import GraphModelClient
from clients.graph_definition_builder import GraphDefinitionBuilder

# ============================================================
# CONFIGURATION
# ============================================================
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
ONTOLOGY_NAME = "Bicycle_Ontology_Model_New"
GRAPH_MODEL_NAME = "Bicycle_Ontology_Model_New_graph"
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
    print(f"  [FAIL] Workspace '{name}' not found.  Available: {available}")
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


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Deploy GraphModel from ontology metadata")
    parser.add_argument("--workspace", default=WORKSPACE_NAME,
                        help=f"Target workspace name (default: {WORKSPACE_NAME})")
    parser.add_argument("--graph-model", default=GRAPH_MODEL_NAME,
                        help=f"Graph model name (default: {GRAPH_MODEL_NAME})")
    parser.add_argument("--lakehouse", default=LAKEHOUSE_NAME,
                        help=f"Source lakehouse name (default: {LAKEHOUSE_NAME})")
    parser.add_argument("--update", action="store_true",
                        help="Update existing graph model definition")
    parser.add_argument("--fix-existing", action="store_true",
                        help="Fix an auto-provisioned graph's schema")
    parser.add_argument("--no-refresh", action="store_true",
                        help="Skip waiting for data load after deploy")
    args = parser.parse_args()

    print("=" * 70)
    print("  DEPLOY GRAPH MODEL FROM ONTOLOGY")
    print("=" * 70)
    print()
    print(f"  Workspace:        {args.workspace}")
    print(f"  Graph Model:      {args.graph_model}")
    print(f"  Lakehouse:        {args.lakehouse}")
    print(f"  Ontology Dir:     {ONTOLOGY_DIR}")
    if args.fix_existing:
        print(f"  Mode:             --fix-existing (repair auto-provisioned graph)")
    elif args.update:
        print(f"  Mode:             --update (overwrite existing definition)")
    else:
        print(f"  Mode:             create (skip if exists)")
    print()

    # ── Step 1: Authenticate ──────────────────────────────────
    print("  Step 1: Authenticating...")
    token = get_fabric_token(TENANT_ID, ADMIN_ACCOUNT)
    client = GraphModelClient(token)
    ws_id = get_workspace_id(token, args.workspace)
    lh_id = get_lakehouse_id(token, ws_id, args.lakehouse)
    print(f"    Workspace ID: {ws_id}")
    print(f"    Lakehouse ID: {lh_id}")

    # ── Step 2: Validate ontology folder ──────────────────────
    print(f"\n  Step 2: Loading ontology metadata from disk...")
    if not ONTOLOGY_DIR.exists():
        print(f"    [FAIL] Ontology directory not found: {ONTOLOGY_DIR}")
        sys.exit(1)

    builder = GraphDefinitionBuilder(ONTOLOGY_DIR, ws_id, lh_id)
    try:
        entity_count, rel_count = builder.load_ontology()
    except FileNotFoundError as e:
        print(f"    [FAIL] {e}")
        sys.exit(1)

    # Validate
    entity_issues, rel_issues = builder.validate()
    if entity_issues:
        print(f"    [WARN] {len(entity_issues)} entities with issues:")
        for name, issues in entity_issues[:5]:
            print(f"      - {name}: {', '.join(issues)}")
    if rel_issues:
        print(f"    [WARN] {len(rel_issues)} relationships with issues:")
        for name, issues in rel_issues[:5]:
            print(f"      - {name}: {', '.join(issues)}")

    # ── Step 3: Generate 5-part definition ────────────────────
    print(f"\n  Step 3: Building graph definition...")
    description = f"Graph model for {ONTOLOGY_NAME} — {entity_count} node types, {rel_count} edge types"
    parts = builder.build_all_parts(args.graph_model, description)
    print(f"    Generated {len(parts)} definition parts")

    # ── Step 4: Deploy ────────────────────────────────────────
    result = ""
    gm_id = None

    if args.fix_existing:
        # --fix-existing mode: find the auto-provisioned graph and push
        # the correct definition to fix entity keys, edges, and data sources.
        print(f"\n  Step 4: Fix auto-provisioned graph model")
        existing = client.find_by_name(ws_id, args.graph_model)
        if not existing:
            # Try finding companion graph (ontology name in graph name)
            for item in client.list(ws_id):
                if ONTOLOGY_NAME in item.get("displayName", ""):
                    existing = item
                    print(f"    Found companion: {item['displayName']}")
                    break

        if not existing:
            print(f"    [FAIL] Graph model not found.")
            print(f"    Create the ontology in the Fabric UI first, which")
            print(f"    auto-provisions a graph model. Then re-run with --fix-existing.")
            sys.exit(1)

        gm_id = existing["id"]
        print(f"    Found: {existing['displayName']} ({gm_id})")

        # Show current definition for comparison
        print(f"\n    --- Current definition (before fix) ---")
        current_def = client.get_definition_decoded(ws_id, gm_id)
        if current_def:
            for path, content in current_def.items():
                if path == "graphType.json":
                    nodes = content.get("nodeTypes", []) if isinstance(content, dict) else []
                    edges = content.get("edgeTypes", []) if isinstance(content, dict) else []
                    print(f"      {path}: {len(nodes)} nodeTypes, {len(edges)} edgeTypes")
                elif path == "graphDefinition.json":
                    nt = content.get("nodeTables", []) if isinstance(content, dict) else []
                    et = content.get("edgeTables", []) if isinstance(content, dict) else []
                    print(f"      {path}: {len(nt)} nodeTables, {len(et)} edgeTables")
                elif path == "dataSources.json":
                    ds = content.get("dataSources", []) if isinstance(content, dict) else []
                    print(f"      {path}: {len(ds)} data sources")
                else:
                    chars = len(json.dumps(content)) if isinstance(content, dict) else len(str(content))
                    print(f"      {path}: {chars:,} chars")

        # Push the corrected definition
        print(f"\n    --- Pushing corrected definition ---")
        ok = client.update_definition(ws_id, gm_id, parts)
        if ok:
            result = "[OK] Definition fixed via updateDefinition"
        else:
            result = "[FAIL] updateDefinition failed"

    else:
        existing = client.find_by_name(ws_id, args.graph_model)

        if existing and args.update:
            gm_id = existing["id"]
            print(f"\n  Step 4: Updating existing GraphModel ({gm_id})")
            ok = client.update_definition(ws_id, gm_id, parts)
            result = "[OK] Definition updated" if ok else "[FAIL] Update failed"

        elif existing and not args.update:
            gm_id = existing["id"]
            print(f"\n  Step 4: GraphModel '{args.graph_model}' already exists ({gm_id})")
            print(f"    Use --update to overwrite the definition.")
            print(f"    Use --fix-existing to repair an auto-provisioned graph.")
            result = "[SKIP] Already exists"

        else:
            print(f"\n  Step 4: Creating GraphModel '{args.graph_model}'")
            gm_id = client.create(ws_id, args.graph_model, description,
                                  parts=parts)
            if gm_id:
                result = f"[OK] Created ({gm_id})"
            else:
                result = "[FAIL] Create failed"
                gm_id = None

    # ── Step 5: Wait for data load ────────────────────────────
    if gm_id and not args.no_refresh and "FAIL" not in result and "SKIP" not in result:
        print(f"\n  Step 5: Waiting for data load")
        loaded = client.wait_for_data_load(ws_id, gm_id, timeout=600)
        if loaded:
            result += " + data loaded"
        else:
            print(f"    [WARN] Data load incomplete — check job history")

    # ── Step 6: Verify with GQL query ─────────────────────────
    if gm_id and "FAIL" not in result and "SKIP" not in result:
        print(f"\n  Step 6: Verify graph via GQL queries")

        # Check queryable schema
        schema = client.get_queryable_graph_type(ws_id, gm_id)
        if schema:
            node_types = schema.get("nodeTypes", [])
            edge_types = schema.get("edgeTypes", [])
            print(f"    Queryable schema: {len(node_types)} node types, "
                  f"{len(edge_types)} edge types")
        else:
            print(f"    [WARN] Could not retrieve queryable schema (graph may still be loading)")

        # Count nodes
        node_count = client.execute_query(
            ws_id, gm_id, "MATCH (n) RETURN count(n) AS total")
        if node_count:
            rows = node_count.get("results", [{}])
            if rows:
                print(f"    Node count: {rows}")

    elif gm_id and "SKIP" in result:
        print(f"\n  Step 6: Checking current status")
        item = client.get(ws_id, gm_id)
        if item:
            props = item.get("properties", {})
            readiness = props.get("queryReadiness", "Unknown")
            loading = props.get("lastDataLoadingStatus") or {}
            status = loading.get("status", "Unknown")
            print(f"    queryReadiness: {readiness}")
            print(f"    loadingStatus:  {status}")

    # ── Summary ───────────────────────────────────────────────
    summary = builder.get_summary()
    print()
    print("=" * 70)
    print("  DEPLOYMENT SUMMARY")
    print("=" * 70)
    print(f"  {args.graph_model:<45} {result}")
    print()
    print("  Graph model definition:")
    print(f"    Nodes:  {summary['valid_entities']} types  "
          f"({', '.join(summary['entity_names'][:5])}...)")
    print(f"    Edges:  {summary['valid_relationships']} types")
    print()

    if args.fix_existing:
        print("  " + "-" * 66)
        print("  --fix-existing mode: The auto-provisioned graph has been repaired.")
        print("  Entity keys, edge contextualizations, and data sources are now")
        print("  aligned with the ontology definition.")
        print()
    else:
        print("  " + "-" * 66)
        print("  NOTE: This is a STANDALONE GraphModel, independent of the Ontology.")
        print("  The Fabric Preview API does not support linking them.")
        print("  To fix an auto-provisioned graph, re-run with --fix-existing.")
        print()

    print("  Next steps:")
    print("    - Open Fabric workspace → Graph view to explore visually")
    print("    - Run GQL queries via executeQuery API or Fabric UI")
    print()

    if "FAIL" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
