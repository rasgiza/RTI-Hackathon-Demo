#!/usr/bin/env python3
"""
deploy.py — Deploy all 23 Bicycle RTI items to a Fabric workspace.

Usage:
    1. pip install fabric-cicd azure-identity
    2. python deploy.py

You will be prompted to sign in via browser (InteractiveBrowserCredential).
Items are deployed in 5 staged rounds to respect dependencies.
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

# Source workspace GUID baked into exported files — replaced at deploy time
SOURCE_WORKSPACE_ID = "573cc7c7-a45a-4fd9-886e-9db4e9798db4"
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
        if len(user_input) == 36 and user_input.count("-") == 4:
            ws_id = user_input
        else:
            ws_name = user_input

    # Authenticate via browser
    print("\n🔐 Authenticating... (a browser window will open)")
    credential = InteractiveBrowserCredential()

    # Auto-detect user email for Activator alerts
    ALERT_PLACEHOLDER = "__ALERT_RECIPIENT_EMAIL__"
    alert_email = ""
    try:
        tok = credential.get_token("https://api.fabric.microsoft.com/.default").token
        payload = tok.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.b64decode(payload))
        alert_email = claims.get("upn") or claims.get("unique_name") or claims.get("preferred_username", "")
        if alert_email:
            print(f"   📧 Alert email (from token): {alert_email}")
    except Exception:
        print("   ⚠️ Could not detect email — Activator alerts will keep placeholder")

    import tempfile, shutil

    # Resolve target workspace ID for GUID replacement
    target_ws_id = ws_id  # may be None if using ws_name
    if not target_ws_id and ws_name:
        # We'll get it after first deploy; for now just note it
        print(f"   ℹ️  Workspace GUID replacement will use resolved ID")

    # ── Helper: replace source workspace GUID with target ──
    TEXT_EXTENSIONS = {".json", ".tmdl", ".pbism", ".ipynb", ".xml", ".yml", ".yaml", ".md"}

    def patch_workspace_guid(stage_ws_path, target_id):
        """Replace SOURCE_WORKSPACE_ID with target_id in all text files."""
        if not target_id or target_id == SOURCE_WORKSPACE_ID:
            return 0
        count = 0
        for root, dirs, files in os.walk(stage_ws_path):
            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in TEXT_EXTENSIONS and fname != ".platform":
                    continue
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        content = f.read()
                    if SOURCE_WORKSPACE_ID in content:
                        content = content.replace(SOURCE_WORKSPACE_ID, target_id)
                        with open(fpath, "w", encoding="utf-8") as f:
                            f.write(content)
                        count += 1
                except (UnicodeDecodeError, PermissionError):
                    continue
        if count:
            print(f"   🔄 Patched workspace GUID in {count} file(s)")
        return count

    def patch_alert_email(stage_ws_path):
        """Replace __ALERT_RECIPIENT_EMAIL__ with user's email."""
        if not alert_email:
            return 0
        count = 0
        for root, dirs, files in os.walk(stage_ws_path):
            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in TEXT_EXTENSIONS and fname != ".platform":
                    continue
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        content = f.read()
                    if ALERT_PLACEHOLDER in content:
                        content = content.replace(ALERT_PLACEHOLDER, alert_email)
                        with open(fpath, "w", encoding="utf-8") as f:
                            f.write(content)
                        count += 1
                except (UnicodeDecodeError, PermissionError):
                    continue
        if count:
            print(f"   📧 Patched alert email in {count} file(s)")
        return count

    # ── Helper: make isolated stage dir ──
    def make_stage_dir(type_list, ref_types=None):
        """Copy folders for given item types. For ref_types, copy only .platform files.
        Replaces source workspace GUID with target workspace ID."""
        sd = tempfile.mkdtemp(prefix="rti_stage_")
        sw = os.path.join(sd, "workspace")
        os.makedirs(sw)
        folders = []
        for t in type_list:
            folders.extend(item_index.get(t, []))
        for f in folders:
            shutil.copytree(os.path.join(workspace_dir, f), os.path.join(sw, f))
        # Add reference-only .platform files for logicalId resolution
        if ref_types:
            for t in ref_types:
                for f in item_index.get(t, []):
                    if os.path.exists(os.path.join(sw, f)):
                        continue  # Already copied as full folder
                    ref_dir = os.path.join(sw, f)
                    os.makedirs(ref_dir, exist_ok=True)
                    src_platform = os.path.join(workspace_dir, f, ".platform")
                    if os.path.exists(src_platform):
                        shutil.copy2(src_platform, os.path.join(ref_dir, ".platform"))
        # Replace source workspace GUID with target
        patch_workspace_guid(sw, ws_id)
        patch_alert_email(sw)
        return sd, sw, folders

    # ═══════════════════════════════════════════════════════════
    # STAGE 1/5: Lakehouses
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print("🚀 Stage 1/5: Lakehouses")
    print(f"{'='*60}")
    stage_dir, stage_ws, folders = make_stage_dir(["Lakehouse"])
    print(f"   📦 {', '.join(f.rsplit('.', 1)[0] for f in folders)}")
    kwargs = {"repository_directory": stage_ws, "item_type_in_scope": ["Lakehouse"],
              "token_credential": credential}
    if ws_id:
        kwargs["workspace_id"] = ws_id
    else:
        kwargs["workspace_name"] = ws_name
    publish_all_items(FabricWorkspace(**kwargs))
    shutil.rmtree(stage_dir, ignore_errors=True)
    print("   ✅ Stage 1/5 complete")

    # ═══════════════════════════════════════════════════════════
    # STAGE 2/5: Eventhouse
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print("🚀 Stage 2/5: Eventhouse")
    print(f"{'='*60}")
    stage_dir, stage_ws, folders = make_stage_dir(["Eventhouse"])
    print(f"   📦 {', '.join(f.rsplit('.', 1)[0] for f in folders)}")
    kwargs = {"repository_directory": stage_ws, "item_type_in_scope": ["Eventhouse"],
              "token_credential": credential}
    if ws_id:
        kwargs["workspace_id"] = ws_id
    else:
        kwargs["workspace_name"] = ws_name
    publish_all_items(FabricWorkspace(**kwargs))
    shutil.rmtree(stage_dir, ignore_errors=True)
    print("   ✅ Stage 2/5 complete")

    # ═══════════════════════════════════════════════════════════
    # STAGE 3/5: KQL Database (REST API — bypasses fabric-cicd)
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print("🚀 Stage 3/5: KQL Database (via REST API)")
    print(f"{'='*60}")

    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Resolve ws_id if we only have name
    target_ws_id = ws_id
    if not target_ws_id:
        target_ws_id = _resolve_workspace_id(hdrs, ws_name)

    # Find deployed Eventhouse
    resp = requests.get(f"{API}/workspaces/{target_ws_id}/eventhouses", headers=hdrs)
    resp.raise_for_status()
    eventhouse_id = None
    for eh in resp.json().get("value", []):
        if eh["displayName"] == "bikerentaleventhouse":
            eventhouse_id = eh["id"]
            break
    if not eventhouse_id:
        raise RuntimeError("❌ Eventhouse not found")
    print(f"   Found Eventhouse: {eventhouse_id}")

    # Check if KQL DB exists
    resp = requests.get(f"{API}/workspaces/{target_ws_id}/kqlDatabases", headers=hdrs)
    resp.raise_for_status()
    kqldb_id = None
    for db in resp.json().get("value", []):
        if db["displayName"] == "bikerentaleventhouse":
            kqldb_id = db["id"]
            break

    if kqldb_id:
        print(f"   ℹ️  KQL Database already exists: {kqldb_id}")
    else:
        print("   Creating KQL Database...")
        create_body = {
            "displayName": "bikerentaleventhouse",
            "creationPayload": {
                "databaseType": "ReadWrite",
                "parentEventhouseItemId": eventhouse_id,
                "oneLakeCachingPeriod": "P36500D",
                "oneLakeStandardStoragePeriod": "P36500D",
            },
        }
        resp = requests.post(f"{API}/workspaces/{target_ws_id}/kqlDatabases",
                             headers=hdrs, json=create_body)
        result = _wait_lro(resp, hdrs, "Create KQLDatabase")
        if resp.status_code in (200, 201):
            kqldb_id = resp.json().get("id")
        elif result:
            resp2 = requests.get(f"{API}/workspaces/{target_ws_id}/kqlDatabases", headers=hdrs)
            for db in resp2.json().get("value", []):
                if db["displayName"] == "bikerentaleventhouse":
                    kqldb_id = db["id"]
                    break
        if not kqldb_id:
            raise RuntimeError("❌ Failed to create KQL Database")
        print(f"   ✅ Created: {kqldb_id}")

    # Run schema KQL commands
    print("   Running schema commands...")
    schema_path = os.path.join(workspace_dir,
                               "bikerentaleventhouse.KQLDatabase", "DatabaseSchema.kql")
    if os.path.exists(schema_path):
        # Wait for query URI to become available (may take a few seconds after creation)
        query_uri = None
        print("   Waiting for KQL Database query URI...")
        for attempt in range(12):  # Retry up to ~60 seconds
            resp = requests.get(f"{API}/workspaces/{target_ws_id}/kqlDatabases/{kqldb_id}",
                                headers=hdrs)
            if resp.status_code == 200:
                props = resp.json().get("properties", {})
                query_uri = props.get("queryUri") or props.get("parentEventhouseUri")
                if query_uri:
                    break
            if attempt < 11:
                time.sleep(5)

        if query_uri:
            print(f"   Query URI: {query_uri}")
            with open(schema_path, "r", encoding="utf-8") as sf:
                schema_text = sf.read()
            commands = []
            current = []
            for line in schema_text.split("\n"):
                s = line.strip()
                if s.startswith(".") and current:
                    commands.append("\n".join(current))
                    current = [line]
                elif s and not s.startswith("//"):
                    current.append(line)
            if current:
                commands.append("\n".join(current))
            # Use Kusto-scoped token for management commands
            kusto_token = credential.get_token("https://kusto.kusto.windows.net/.default").token
            kusto_hdrs = {"Authorization": f"Bearer {kusto_token}", "Content-Type": "application/json"}
            ok = 0
            for cmd in commands:
                c = cmd.strip()
                if not c or c.startswith("//"):
                    continue
                try:
                    r = requests.post(f"{query_uri}/v1/rest/mgmt", headers=kusto_hdrs,
                                      json={"csl": c, "db": "bikerentaleventhouse"})
                    if r.status_code == 200:
                        ok += 1
                    else:
                        print(f"   ⚠️ Command failed ({r.status_code}): {c.split(chr(10))[0][:60]}...")
                except Exception as e:
                    print(f"   ⚠️ Command error: {e}")
            print(f"   ✅ Schema: {ok}/{len(commands)} commands succeeded")
        else:
            print("   ⚠️ Could not get query URI after 60s — schema will need manual setup")
    print("   ✅ Stage 3/5 complete")

    # ═══════════════════════════════════════════════════════════
    # STAGE 4/5: Semantic Models + Notebooks
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print("🚀 Stage 4/5: Semantic Models + Notebooks")
    print(f"{'='*60}")
    stage_dir, stage_ws, _ = make_stage_dir(["SemanticModel", "Notebook"],
                                            ref_types=["KQLDatabase", "Lakehouse", "Eventhouse"])
    deploy_types = ["SemanticModel", "Notebook"]
    print(f"   📦 Deploying: {', '.join(f.rsplit('.', 1)[0] for f in sum([item_index.get(t, []) for t in deploy_types], []))}")
    kwargs = {"repository_directory": stage_ws, "item_type_in_scope": deploy_types,
              "token_credential": credential}
    if ws_id:
        kwargs["workspace_id"] = ws_id
    else:
        kwargs["workspace_name"] = ws_name
    publish_all_items(FabricWorkspace(**kwargs))
    shutil.rmtree(stage_dir, ignore_errors=True)
    print("   ✅ Stage 4/5 complete")

    # ═══════════════════════════════════════════════════════════
    # STAGE 5/5: Eventstreams + Analytics + Presentation
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print("🚀 Stage 5/5: Eventstreams + Analytics + Presentation")
    print(f"{'='*60}")
    stage_dir, stage_ws, _ = make_stage_dir(["Eventstream", "DataPipeline", "KQLDashboard", "DataAgent"],
                                            ref_types=["KQLDatabase", "Lakehouse", "Eventhouse"])
    # Selectively add BicycleFleet_Activator (not Cycling Campaign — needs ontology)
    bf_reflex = "BicycleFleet_Activator.Reflex"
    bf_src = os.path.join(workspace_dir, bf_reflex)
    if os.path.isdir(bf_src):
        shutil.copytree(bf_src, os.path.join(stage_ws, bf_reflex))
        patch_workspace_guid(os.path.join(stage_ws, bf_reflex), ws_id)
        patch_alert_email(os.path.join(stage_ws, bf_reflex))
    deploy_types = ["Eventstream", "DataPipeline",
                    "KQLDashboard", "DataAgent", "Reflex"]
    print(f"   📦 Deploying: {', '.join(f.rsplit('.', 1)[0] for f in sum([item_index.get(t, []) for t in deploy_types], []))}")
    kwargs = {"repository_directory": stage_ws, "item_type_in_scope": deploy_types,
              "token_credential": credential}
    if ws_id:
        kwargs["workspace_id"] = ws_id
    else:
        kwargs["workspace_name"] = ws_name
    publish_all_items(FabricWorkspace(**kwargs))
    shutil.rmtree(stage_dir, ignore_errors=True)
    print("   ✅ Stage 5/5 complete")

    print(f"\n{'='*60}")
    print("✅ ALL 24 ITEMS DEPLOYED SUCCESSFULLY")
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
