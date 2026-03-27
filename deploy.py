#!/usr/bin/env python3
"""
deploy.py — Deploy all 26 Bicycle RTI items to a Fabric workspace.

Usage:
    1. pip install fabric-cicd azure-identity
    2. python deploy.py

You will be prompted to sign in via browser (InteractiveBrowserCredential).
Items are deployed in 4 staged rounds to respect dependencies.
"""

import os
import sys

# ── Check dependencies ──────────────────────────────────────────────
try:
    from fabric_cicd import FabricWorkspace, publish_all_items
    from azure.identity import InteractiveBrowserCredential
except ImportError:
    print("❌ Missing dependencies. Run this first:")
    print("   pip install fabric-cicd azure-identity")
    sys.exit(1)

# ── Configuration ───────────────────────────────────────────────────
WORKSPACE_NAME = None   # Set your workspace name, OR
WORKSPACE_ID   = None   # Set your workspace ID (takes precedence)

# If both are None, you'll be prompted to enter one.
# ────────────────────────────────────────────────────────────────────

import requests, json, base64, time

API = "https://api.fabric.microsoft.com/v1"


def _get_token(credential):
    """Get a bearer token from the credential."""
    return credential.get_token("https://api.fabric.microsoft.com/.default").token


def _wait_lro(resp, headers, label, max_wait=120):
    """Wait for a 202 long-running Fabric API operation."""
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code != 202:
        return None
    op_url = resp.headers.get("Location")
    if not op_url:
        time.sleep(10)
        return None
    retry_after = int(resp.headers.get("Retry-After", 5))
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(retry_after)
        elapsed += retry_after
        op_resp = requests.get(op_url, headers=headers)
        if op_resp.status_code == 200:
            result = op_resp.json()
            status = result.get("status", "")
            if status == "Succeeded":
                return result.get("definition", result)
            if status in ("Failed", "Cancelled"):
                print(f"   ⚠️ {label}: {status}")
                return None
    print(f"   ⚠️ {label}: timed out after {max_wait}s")
    return None


def _resolve_workspace_id(headers, ws_id_or_name):
    """Resolve a workspace ID from ID or name."""
    # If it looks like a GUID, return as-is
    if isinstance(ws_id_or_name, str) and len(ws_id_or_name) == 36 and ws_id_or_name.count("-") == 4:
        return ws_id_or_name
    # Otherwise, search by name
    resp = requests.get(f"{API}/workspaces", headers=headers)
    if resp.status_code == 200:
        for ws in resp.json().get("value", []):
            if ws["displayName"] == ws_id_or_name:
                return ws["id"]
    return None


def auto_fix_placeholders(ws_id_or_name, credential):
    """
    Post-deployment fix: resolve placeholders that fabric-cicd can't handle.
    1. KQL Dashboard __EVENTHOUSE_QUERY_URI__ → real Eventhouse query URI
    2. Pipeline __SM_REFRESH_CONN__ → remove broken SM refresh activity
    """
    token = _get_token(credential)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    ws_id = _resolve_workspace_id(headers, ws_id_or_name)
    if not ws_id:
        print(f"   ❌ Could not resolve workspace: {ws_id_or_name}")
        return

    # ── Step 1: Discover Eventhouse query URI ──
    print("\n📡 Step 1: Discovering Eventhouse query URI...")
    query_uri = None

    resp = requests.get(f"{API}/workspaces/{ws_id}/items?type=Eventhouse", headers=headers)
    eventhouse_id = None
    if resp.status_code == 200:
        for item in resp.json().get("value", []):
            if item["displayName"] == "bikerentaleventhouse":
                eventhouse_id = item["id"]
                break

    if eventhouse_id:
        resp = requests.get(
            f"{API}/workspaces/{ws_id}/eventhouses/{eventhouse_id}", headers=headers
        )
        if resp.status_code == 200:
            props = resp.json().get("properties", {})
            query_uri = props.get("queryServiceUri") or props.get("uri")

    if not query_uri:
        resp = requests.get(
            f"{API}/workspaces/{ws_id}/items?type=KQLDatabase", headers=headers
        )
        if resp.status_code == 200:
            for item in resp.json().get("value", []):
                detail = requests.get(
                    f"{API}/workspaces/{ws_id}/kqlDatabases/{item['id']}", headers=headers
                )
                if detail.status_code == 200:
                    kql_props = detail.json().get("properties", {})
                    query_uri = kql_props.get("queryUri") or kql_props.get("parentEventhouseUri")
                    if query_uri:
                        break

    if query_uri:
        print(f"   ✅ Eventhouse query URI: {query_uri}")
    else:
        print("   ❌ Could not discover query URI — KQL Dashboard needs manual fix")

    # ── Step 2: Patch KQL Dashboard ──
    if query_uri:
        print("\n📊 Step 2: Patching KQL Dashboard clusterUri...")
        resp = requests.get(
            f"{API}/workspaces/{ws_id}/items?type=KQLDashboard", headers=headers
        )
        dashboard_id = None
        if resp.status_code == 200:
            for item in resp.json().get("value", []):
                name = item["displayName"]
                if "Fleet Intelligence" in name or "Live Operations" in name:
                    dashboard_id = item["id"]
                    break

        if dashboard_id:
            resp = requests.post(
                f"{API}/workspaces/{ws_id}/items/{dashboard_id}/getDefinition",
                headers=headers,
            )
            definition = _wait_lro(resp, headers, "Get Dashboard Definition")

            if definition:
                parts = definition.get("definition", definition).get("parts", [])
                patched = False
                new_parts = []
                for part in parts:
                    if part.get("path") == "RealTimeDashboard.json":
                        raw = base64.b64decode(part["payload"]).decode("utf-8")
                        if "__EVENTHOUSE_QUERY_URI__" in raw:
                            raw = raw.replace("__EVENTHOUSE_QUERY_URI__", query_uri)
                            patched = True
                        part = dict(part)
                        part["payload"] = base64.b64encode(raw.encode("utf-8")).decode("utf-8")
                    new_parts.append(part)

                if patched:
                    update_body = {"definition": {"parts": new_parts}}
                    resp = requests.post(
                        f"{API}/workspaces/{ws_id}/items/{dashboard_id}/updateDefinition",
                        headers=headers,
                        json=update_body,
                    )
                    result = _wait_lro(resp, headers, "Update Dashboard")
                    if resp.status_code in (200, 201) or result:
                        print("   ✅ KQL Dashboard patched — queries now point to your Eventhouse")
                    else:
                        print(f"   ⚠️ Dashboard update returned HTTP {resp.status_code}")
                else:
                    print("   ℹ️  Dashboard already has correct clusterUri")
            else:
                print("   ⚠️ Could not retrieve dashboard definition")
        else:
            print("   ⚠️ KQL Dashboard not found in workspace")
    else:
        print("\n📊 Step 2: SKIPPED (no query URI)")

    # ── Step 3: Patch Pipeline (remove broken SM refresh) ──
    print("\n🔧 Step 3: Patching Pipeline (removing broken SM refresh)...")
    resp = requests.get(
        f"{API}/workspaces/{ws_id}/items?type=DataPipeline", headers=headers
    )
    pipeline_id = None
    if resp.status_code == 200:
        for item in resp.json().get("value", []):
            if item["displayName"] == "PL_BicycleRTI_Medallion":
                pipeline_id = item["id"]
                break

    if pipeline_id:
        resp = requests.post(
            f"{API}/workspaces/{ws_id}/items/{pipeline_id}/getDefinition",
            headers=headers,
        )
        definition = _wait_lro(resp, headers, "Get Pipeline Definition")

        if definition:
            parts = definition.get("definition", definition).get("parts", [])
            patched = False
            new_parts = []
            for part in parts:
                if part.get("path") == "pipeline-content.json":
                    raw = base64.b64decode(part["payload"]).decode("utf-8")
                    pipeline_def = json.loads(raw)
                    activities = pipeline_def.get("properties", {}).get("activities", [])
                    original_count = len(activities)
                    activities = [a for a in activities if a.get("type") != "PBISemanticModelRefresh"]
                    if len(activities) < original_count:
                        pipeline_def["properties"]["activities"] = activities
                        raw = json.dumps(pipeline_def, indent=2)
                        patched = True
                    part = dict(part)
                    part["payload"] = base64.b64encode(raw.encode("utf-8")).decode("utf-8")
                new_parts.append(part)

            if patched:
                update_body = {"definition": {"parts": new_parts}}
                resp = requests.post(
                    f"{API}/workspaces/{ws_id}/items/{pipeline_id}/updateDefinition",
                    headers=headers,
                    json=update_body,
                )
                result = _wait_lro(resp, headers, "Update Pipeline")
                if resp.status_code in (200, 201) or result:
                    print("   ✅ Pipeline patched — SM refresh removed (refresh manually after run)")
                else:
                    print(f"   ⚠️ Pipeline update returned HTTP {resp.status_code}")
            else:
                print("   ℹ️  Pipeline already clean (no SM refresh activity)")
        else:
            print("   ⚠️ Could not retrieve pipeline definition")
    else:
        print("   ⚠️ Pipeline not found")

    print(f"\n   {'='*50}")
    print("   ✅ AUTO-FIX COMPLETE")
    print(f"   {'='*50}")

def main():
    # Resolve workspace/ directory relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    workspace_dir = os.path.join(script_dir, "workspace")

    if not os.path.isdir(workspace_dir):
        print(f"❌ Cannot find workspace/ folder at: {workspace_dir}")
        sys.exit(1)

    # Build item-type index from folder names (e.g. "foo.Lakehouse" → type "Lakehouse")
    all_items = [d for d in os.listdir(workspace_dir)
                 if os.path.isdir(os.path.join(workspace_dir, d))]
    print(f"📂 Found {len(all_items)} items in workspace/")

    item_index = {}
    for folder_name in all_items:
        parts = folder_name.rsplit(".", 1)
        if len(parts) == 2:
            item_index.setdefault(parts[1], []).append(folder_name)

    # Resolve workspace target
    ws_id = WORKSPACE_ID
    ws_name = WORKSPACE_NAME

    if not ws_id and not ws_name:
        print()
        print("Enter your Fabric workspace ID or name.")
        print("  (Find the ID in the Fabric URL: app.fabric.microsoft.com/groups/<workspace-id>)")
        user_input = input("Workspace ID or name: ").strip()
        if not user_input:
            print("❌ No workspace specified. Exiting.")
            sys.exit(1)
        # If it looks like a GUID, treat as ID; otherwise treat as name
        if len(user_input) == 36 and user_input.count("-") == 4:
            ws_id = user_input
        else:
            ws_name = user_input

    # Authenticate via browser
    print("\n🔐 Authenticating... (a browser window will open)")
    credential = InteractiveBrowserCredential()

    # Deploy in 6 stages — each stage gets a PHYSICALLY ISOLATED temp directory
    # containing ONLY that stage's folders plus any reference folders needed
    # for logicalId resolution. fabric-cicd resolves cross-item references
    # by scanning the local directory, so dependent items must be present.
    #
    # Key: KQLDatabase has "parentEventhouseItemId" = Eventhouse logicalId.
    # Eventstreams reference Lakehouse/KQLDatabase logicalIds, etc.
    import tempfile, shutil

    # Each stage: (label, types_to_deploy, extra_reference_types)
    stages = [
        ("Stage 1/6: Lakehouses",          ["Lakehouse"],    []),
        ("Stage 2/6: Eventhouse",           ["Eventhouse"],   []),
        ("Stage 3/6: KQL Database",         ["KQLDatabase"],  ["Eventhouse"]),
        ("Stage 4/6: Notebooks + Streams",  ["Notebook", "Eventstream"],
         ["Lakehouse", "Eventhouse", "KQLDatabase"]),
        ("Stage 5/6: Semantic Models + Pipeline",
         ["SemanticModel", "DataPipeline"],
         ["Lakehouse", "Notebook"]),
        ("Stage 6/6: Report + Dashboard + Agents + Activators",
         ["Report", "KQLDashboard", "DataAgent", "Reflex"],
         ["Lakehouse", "Eventhouse", "KQLDatabase", "SemanticModel"]),
    ]

    for label, item_types, ref_types in stages:
        print(f"\n{'='*60}")
        print(f"🚀 {label}")
        print(f"{'='*60}")

        # Collect primary folders for this stage
        stage_folders = []
        for it in item_types:
            stage_folders.extend(item_index.get(it, []))

        if not stage_folders:
            print(f"   ⏭️  No items for types {item_types} — skipping")
            continue

        # Also include reference folders (for logicalId resolution only)
        ref_folders = []
        for rt in ref_types:
            ref_folders.extend(item_index.get(rt, []))

        all_stage_folders = stage_folders + ref_folders
        print(f"   📦 Deploying: {', '.join(f.rsplit('.', 1)[0] for f in stage_folders)}")
        if ref_folders:
            print(f"   🔗 References: {', '.join(f.rsplit('.', 1)[0] for f in ref_folders)}")

        # Create isolated temp dir with stage + reference folders
        stage_dir = tempfile.mkdtemp(prefix="rti_stage_")
        stage_ws_dir = os.path.join(stage_dir, "workspace")
        os.makedirs(stage_ws_dir)

        for folder_name in all_stage_folders:
            src = os.path.join(workspace_dir, folder_name)
            dst = os.path.join(stage_ws_dir, folder_name)
            shutil.copytree(src, dst)

        kwargs = {"repository_directory": stage_ws_dir,
                  "item_type_in_scope": item_types,
                  "token_credential": credential}
        if ws_id:
            kwargs["workspace_id"] = ws_id
        else:
            kwargs["workspace_name"] = ws_name

        ws = FabricWorkspace(**kwargs)
        publish_all_items(ws)
        print(f"   ✅ {label.split(':')[0]} complete")

        shutil.rmtree(stage_dir, ignore_errors=True)

    print(f"\n{'='*60}")
    print("✅ ALL 26 ITEMS DEPLOYED SUCCESSFULLY")
    print(f"{'='*60}")

    # ── Auto-fix placeholders ──
    print(f"\n{'='*60}")
    print("🔧 AUTO-FIX: Resolving deployment placeholders...")
    print(f"{'='*60}")
    auto_fix_placeholders(ws_id or ws_name, credential)

    print()
    print("Next steps:")
    print("  1. Open PL_BicycleRTI_Medallion pipeline → click Run")
    print("     (first load takes ~15-25 min)")
    print("  2. After pipeline → manually refresh both Semantic Models")
    print("  3. Upload Post_Deploy_Setup.ipynb to your workspace → Run all cells")
    print("     (creates Ontology, Graph Model, Operations Agent)")
    print("  4. Open Graph Model → click 'Refresh now'")
    print("  5. Verify Eventstreams are started")
    print("  6. Test the Data Agent: ask 'Which stations need rebalancing?'")


if __name__ == "__main__":
    main()
