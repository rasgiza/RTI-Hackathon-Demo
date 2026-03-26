"""
Deploy Bicycle RTI Medallion Pipeline to Microsoft Fabric
==========================================================
Creates (or updates) a Data Pipeline that runs:

  Silver Enrich → Silver Weather → Gold Star Schema → ML Forecast → Ontology Filter

in sequence.  Each notebook auto-detects whether to do a full load
or incremental based on watermark + table state, so the SAME pipeline
handles both scenarios — no parameters needed.

Usage:
    python deploy_pipeline.py                       # Create or update the pipeline
    python deploy_pipeline.py --trigger              # Deploy AND trigger a run
    python deploy_pipeline.py --schedule 15           # Deploy + schedule every 15 min
    python deploy_pipeline.py --schedule 15 --trigger # Deploy + schedule + trigger now
    python deploy_pipeline.py --unschedule            # Remove the schedule
"""

import json
import base64
import time
import sys
import argparse
import requests
from pathlib import Path
from fabric_auth import get_fabric_token, get_auth_headers
from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

# ============================================================
# CONFIGURATION
# ============================================================
FABRIC_API = "https://api.fabric.microsoft.com/v1"
PIPELINE_NAME = "PL_BicycleRTI_Medallion"
PIPELINE_JSON = Path(__file__).parent / "pipelines" / f"{PIPELINE_NAME}.json"

# Notebook display names → must match what's deployed in workspace
NOTEBOOK_MAP = {
    "__NB03_ID__":  "03_Silver_Enrich_Transform",
    "__NB03A_ID__": "03a_Silver_Weather_Join",
    "__NB04_ID__":  "04_Gold_Star_Schema",
    "__NB06_ID__":  "06_ML_Demand_Forecast",
    "__NB09_ID__":  "09_Ontology_Neighbourhood_Filter",
}

# Semantic model refresh via Web activity (calls Power BI refresh API)
SEMANTIC_MODEL_MAP = {
    "__ONTOLOGY_SM_ID__": "Bicycle Ontology Model",
}


# ============================================================
# API HELPERS
# ============================================================

def resolve_workspace(headers):
    """Return workspace ID by name."""
    resp = requests.get(f"{FABRIC_API}/workspaces", headers=headers)
    resp.raise_for_status()
    ws = next(
        (w for w in resp.json()["value"] if w["displayName"] == WORKSPACE_NAME),
        None,
    )
    if not ws:
        available = [w["displayName"] for w in resp.json()["value"]]
        print(f"  [FAIL] Workspace '{WORKSPACE_NAME}' not found")
        print(f"    Available: {available}")
        sys.exit(1)
    return ws["id"]


def resolve_notebooks(headers, workspace_id):
    """Return {displayName: id} for all notebooks in workspace."""
    resp = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/items?type=Notebook",
        headers=headers,
    )
    resp.raise_for_status()
    return {nb["displayName"]: nb["id"] for nb in resp.json().get("value", [])}


def resolve_semantic_models(headers, workspace_id):
    """Return {displayName: id} for all semantic models in workspace."""
    resp = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/semanticModels",
        headers=headers,
    )
    if resp.status_code == 200:
        return {sm["displayName"]: sm["id"] for sm in resp.json().get("value", [])}
    return {}


def list_pipelines(headers, workspace_id):
    """Return list of DataPipeline items in workspace."""
    resp = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/items?type=DataPipeline",
        headers=headers,
    )
    if resp.status_code == 200:
        return resp.json().get("value", [])
    return []


def wait_for_operation(headers, response, label, timeout=120):
    """Poll a long-running operation until done."""
    location = response.headers.get("Location")
    if not location:
        time.sleep(5)
        return True

    print(f"    Provisioning {label}...")
    start = time.time()
    while time.time() - start < timeout:
        time.sleep(3)
        r = requests.get(location, headers=headers)
        if r.status_code == 200:
            status = r.json().get("status", "")
            if status == "Succeeded":
                return True
            if status in ("Failed", "Cancelled"):
                print(f"    [FAIL] {label}: {status}")
                return False
        elif r.status_code == 404:
            time.sleep(3)
            return True
    print(f"    [FAIL] {label}: timed out ({timeout}s)")
    return False


# ============================================================
# PIPELINE BUILD
# ============================================================

def build_pipeline_definition(workspace_id, notebook_ids, semantic_model_ids=None):
    """
    Load pipeline JSON template and patch in real workspace + notebook + SM IDs.
    """
    with open(PIPELINE_JSON, "r", encoding="utf-8") as f:
        definition = json.load(f)

    text = json.dumps(definition)

    # Patch workspace ID
    text = text.replace("__WORKSPACE_ID__", workspace_id)

    # Patch notebook IDs
    missing = []
    for placeholder, nb_name in NOTEBOOK_MAP.items():
        nb_id = notebook_ids.get(nb_name)
        if nb_id:
            text = text.replace(placeholder, nb_id)
            print(f"  {nb_name} → {nb_id}")
        else:
            missing.append(nb_name)

    if missing:
        print(f"\n  [FAIL] Notebooks not found in workspace: {missing}")
        print("    Deploy notebooks first: python _redeploy_notebooks.py")
        sys.exit(1)

    # Patch semantic model IDs
    if semantic_model_ids:
        for placeholder, sm_name in SEMANTIC_MODEL_MAP.items():
            sm_id = semantic_model_ids.get(sm_name)
            if sm_id:
                text = text.replace(placeholder, sm_id)
                print(f"  {sm_name} → {sm_id}")
            else:
                print(f"  [WARN] Semantic model '{sm_name}' not found — refresh step will be skipped")

    return json.loads(text)


# ============================================================
# CREATE / UPDATE PIPELINE
# ============================================================

def deploy_pipeline(headers, workspace_id, definition):
    """Create or update the pipeline in Fabric."""
    # Encode definition as base64
    def_json = json.dumps(definition, indent=2)
    def_b64 = base64.b64encode(def_json.encode("utf-8")).decode("utf-8")

    payload_body = {
        "definition": {
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": def_b64,
                    "payloadType": "InlineBase64",
                }
            ]
        }
    }

    # Check if pipeline already exists
    existing = list_pipelines(headers, workspace_id)
    found = next(
        (p for p in existing if p["displayName"] == PIPELINE_NAME), None
    )

    if found:
        # Update existing pipeline
        pipeline_id = found["id"]
        url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{pipeline_id}/updateDefinition"
        resp = requests.post(url, headers=headers, json=payload_body)

        if resp.status_code in (200, 202):
            if resp.status_code == 202:
                wait_for_operation(headers, resp, PIPELINE_NAME)
            print(f"\n  [OK] Pipeline UPDATED: {PIPELINE_NAME} ({pipeline_id})")
            return pipeline_id
        else:
            print(f"\n  [FAIL] Update failed: {resp.status_code}")
            print(f"    {resp.text[:500]}")
            sys.exit(1)
    else:
        # Create new pipeline
        create_body = {
            "displayName": PIPELINE_NAME,
            "type": "DataPipeline",
        }
        create_body.update(payload_body)

        url = f"{FABRIC_API}/workspaces/{workspace_id}/items"
        resp = requests.post(url, headers=headers, json=create_body)

        if resp.status_code in (200, 201):
            pipeline_id = resp.json().get("id")
            print(f"\n  [OK] Pipeline CREATED: {PIPELINE_NAME} ({pipeline_id})")
            return pipeline_id
        elif resp.status_code == 202:
            success = wait_for_operation(headers, resp, PIPELINE_NAME)
            if success:
                items = list_pipelines(headers, workspace_id)
                item = next(
                    (p for p in items if p["displayName"] == PIPELINE_NAME), None
                )
                if item:
                    print(f"\n  [OK] Pipeline CREATED: {PIPELINE_NAME} ({item['id']})")
                    return item["id"]
            print(f"\n  [FAIL] Pipeline creation failed")
            sys.exit(1)
        else:
            print(f"\n  [FAIL] Create failed: {resp.status_code}")
            print(f"    {resp.text[:500]}")
            sys.exit(1)


# ============================================================
# TRIGGER PIPELINE RUN
# ============================================================

def trigger_run(headers, workspace_id, pipeline_id):
    """Trigger the pipeline and monitor until completion."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
    resp = requests.post(url, headers=headers)

    if resp.status_code not in (200, 202):
        print(f"  [FAIL] Could not trigger pipeline: {resp.status_code}")
        print(f"    {resp.text[:300]}")
        return

    print(f"\n  [OK] Pipeline run triggered!")
    print(f"    Open Fabric UI → Pipelines → {PIPELINE_NAME} to monitor progress")

    # Try to get the run ID from Location header
    location = resp.headers.get("Location")
    if location:
        print(f"    Monitor URL: {location}")


# ============================================================
# SCHEDULE MANAGEMENT
# ============================================================

def set_schedule(headers, workspace_id, pipeline_id, interval_minutes):
    """
    Configure a recurring schedule for the pipeline.
    Uses Fabric Job Scheduler API.
    """
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{pipeline_id}/jobScheduler"

    # Build the schedule: run every N minutes, starting now
    schedule_payload = {
        "enabled": True,
        "configuration": {
            "type": "Pipeline"
        },
        "type": "Cron",
        "cronExpression": _minutes_to_cron(interval_minutes),
        "startDateTime": None,   # start immediately
        "endDateTime": None,     # no end
        "timeZoneId": "UTC",
    }

    # Try create first, then update if it already exists
    resp = requests.post(url, headers=headers, json=schedule_payload)

    if resp.status_code == 409:  # Schedule already exists → update
        resp = requests.patch(url, headers=headers, json=schedule_payload)

    if resp.status_code in (200, 201):
        print(f"\n  [OK] Schedule SET: every {interval_minutes} minutes")
        print(f"    Cron: {schedule_payload['cronExpression']}")
        print(f"    Timezone: UTC")
        print(f"\n    How catch-up works:")
        print(f"    • Eventstream writes to Bronze continuously (~1/sec)")
        print(f"    • Every {interval_minutes} min, pipeline wakes up")
        print(f"    • Watermark filter: only NEW rows since last run")
        print(f"    • If no new data → notebooks skip in seconds")
        print(f"    • If pipeline was down 2 hours → catches up all data in one batch")
        return True
    else:
        print(f"\n  [WARN] Could not set schedule via API: {resp.status_code}")
        print(f"    {resp.text[:400]}")
        print(f"\n    Manual setup: Fabric UI → Pipeline → Schedule tab")
        print(f"    Set recurring: every {interval_minutes} minutes")
        return False


def remove_schedule(headers, workspace_id, pipeline_id):
    """Disable the pipeline schedule."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{pipeline_id}/jobScheduler"

    disable_payload = {"enabled": False}
    resp = requests.patch(url, headers=headers, json=disable_payload)

    if resp.status_code in (200, 204):
        print(f"\n  [OK] Schedule DISABLED for {PIPELINE_NAME}")
        return True
    elif resp.status_code == 404:
        print(f"\n  [OK] No schedule found — nothing to disable")
        return True
    else:
        print(f"\n  [WARN] Could not disable schedule: {resp.status_code}")
        print(f"    {resp.text[:300]}")
        return False


def _minutes_to_cron(minutes):
    """
    Convert an interval in minutes to a cron expression.
    Examples:
        5  → '*/5 * * * *'   (every 5 minutes)
        15 → '*/15 * * * *'  (every 15 minutes)
        30 → '*/30 * * * *'  (every 30 minutes)
        60 → '0 * * * *'     (every hour on the hour)
    """
    if minutes <= 0 or minutes > 1440:
        raise ValueError(f"Schedule interval must be 1-1440 minutes, got {minutes}")
    if minutes == 60:
        return "0 * * * *"
    if minutes == 1440:
        return "0 0 * * *"
    if 60 % minutes == 0:
        return f"*/{minutes} * * * *"
    # For non-standard intervals, use every N minutes
    return f"*/{minutes} * * * *"


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Deploy Bicycle RTI Pipeline")
    parser.add_argument("--trigger", action="store_true",
                        help="Also trigger a pipeline run after deploy")
    parser.add_argument("--schedule", type=int, metavar="MINUTES",
                        help="Schedule pipeline to run every N minutes (e.g. 15)")
    parser.add_argument("--unschedule", action="store_true",
                        help="Remove the pipeline schedule")
    args = parser.parse_args()

    print("=" * 60)
    print("Deploy Bicycle RTI Medallion Pipeline")
    print("=" * 60)

    # Authenticate
    token = get_fabric_token(TENANT_ID, ADMIN_ACCOUNT)
    headers = get_auth_headers(token)

    # Resolve workspace
    workspace_id = resolve_workspace(headers)
    print(f"Workspace: {WORKSPACE_NAME} ({workspace_id})")

    # Resolve notebook IDs
    print(f"\nResolving notebook IDs...")
    notebook_ids = resolve_notebooks(headers, workspace_id)

    # Resolve semantic model IDs
    print(f"\nResolving semantic model IDs...")
    semantic_model_ids = resolve_semantic_models(headers, workspace_id)

    # Build pipeline definition with real IDs
    print(f"\nBuilding pipeline definition...")
    definition = build_pipeline_definition(workspace_id, notebook_ids, semantic_model_ids)

    # Deploy
    print(f"\nDeploying pipeline...")
    pipeline_id = deploy_pipeline(headers, workspace_id, definition)

    # Pipeline execution order
    print(f"\n  Pipeline: {PIPELINE_NAME}")
    print(f"  ┌──────────────────────────────────────────────┐")
    print(f"  │  1. Silver Enrich Transform (NB03)            │")
    print(f"  │         ↓ on success                          │")
    print(f"  │  2. Silver Weather Join (NB03a)               │")
    print(f"  │         ↓ on success                          │")
    print(f"  │  3. Gold Star Schema (NB04)                   │")
    print(f"  │         ↓ on success                          │")
    print(f"  │  4. ML Demand Forecast (NB06)                 │")
    print(f"  │         ↓ on success                          │")
    print(f"  │  5. Ontology Neighbourhood Filter (NB09)      │")
    print(f"  │         (includes SM refresh via notebook)     │")
    print(f"  └──────────────────────────────────────────────┘")
    print(f"  Mode: Auto-detect (full / incremental / skip)")

    # Schedule management
    if args.unschedule:
        remove_schedule(headers, workspace_id, pipeline_id)
    elif args.schedule:
        set_schedule(headers, workspace_id, pipeline_id, args.schedule)

    # Optionally trigger
    if args.trigger:
        print(f"\nTriggering pipeline run...")
        trigger_run(headers, workspace_id, pipeline_id)
    elif not args.unschedule:
        print(f"\n  To trigger a run:")
        print(f"    python deploy_pipeline.py --trigger")
        print(f"    python deploy_pipeline.py --schedule 15   (every 15 min)")
        print(f"    OR open Fabric UI → Pipelines → {PIPELINE_NAME} → Run")


if __name__ == "__main__":
    main()
