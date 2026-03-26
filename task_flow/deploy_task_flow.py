"""
deploy_task_flow.py
-------------------
Helper to set up the Fabric Task Flow for the Bicycle RTI workspace.

Since there is NO REST API for task flows (they are a workspace-level UI feature),
this script:
  1. Validates all workspace items exist
  2. Prints a mapping of tasks → items for manual assignment
  3. Generates a clean JSON file ready for import via the Fabric UI

Usage:
    python deploy_task_flow.py

After running, import the task flow via Fabric UI:
    Workspace → List View → Task flow area → Import a task flow → Select JSON file
"""

import os, sys, json, requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, ".."))

try:
    from fabric_auth import get_fabric_token, get_auth_headers
    from config import TENANT_ID, ADMIN_ACCOUNT
except ImportError:
    sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "scripts"))
    from fabric_auth import get_fabric_token, get_auth_headers
    from config import TENANT_ID, ADMIN_ACCOUNT

WORKSPACE_ID = "573cc7c7-a45a-4fd9-886e-9db4e9798db4"

# ---- Task → Item mapping (matches the screenshot task flow) ----
TASK_ITEMS = {
    "Raw streaming Bike Rentals": {
        "task_type": "Get data",
        "items": ["RTIbikeRental"],
    },
    "Raw Streaming Weather": {
        "task_type": "Get data",
        "items": ["RTI-WeatherDemo"],
    },
    "Eventhouse": {
        "task_type": "Store data",
        "items": ["bikerentaleventhouse"],
    },
    "RTI Dashboard": {
        "task_type": "Visualize data",
        "items": [
            "Bicycle Fleet Intelligence — Live Operations",
        ],
    },
    "Bronze data": {
        "task_type": "Store data",
        "items": [
            "bikerental_bronze_raw",
            "weather_bronze_raw",
            "02_Bronze_Streaming_Ingest",
            "03a_Silver_Weather_Join",
        ],
    },
    "Silver data": {
        "task_type": "Store data",
        "items": [
            "bicycles_silver",
            "03_Silver_Enrich_Transform",
            "03a_Silver_Weather_Join",
        ],
    },
    "Golden data": {
        "task_type": "Store data",
        "items": [
            "bicycles_gold",
            "04_Gold_Star_Schema",
        ],
    },
    "Analyze and train": {
        "task_type": "Analyze and train data",
        "items": [
            "05_KQL_Realtime_Queries",
            "06_ML_Demand_Forecast",
            "08_GeoAnalytics_HotSpots",
        ],
    },
    "Semantic Model": {
        "task_type": "General",
        "items": [
            "Bicycle RTI Analytics",
            "Bicycle Ontology Model",
        ],
    },
    "Ontology": {
        "task_type": "General",
        "items": [
            "Bicycle_Ontology_Model_New",
            "Bicycle_Ontology_Model_New_graph",
            "09_Ontology_Neighbourhood_Filter",
        ],
    },
    "Realtime activation": {
        "task_type": "Track data",
        "items": [
            "BicycleFleet_Activator",
            "Cycling Campaign Activator",
        ],
    },
    "PowerBI Dashboard": {
        "task_type": "Visualize data",
        "items": ["Bicycle Fleet Operations Report"],
    },
    "Data Agent": {
        "task_type": "General",
        "items": [
            "Bicycle Fleet Intelligence Agent",
            "ontology data agent",
            "Cycling-Campaign-Agent",
        ],
    },
    "Power Automate Connector": {
        "task_type": "General",
        "items": [],
    },
    "Teams": {
        "task_type": "General",
        "items": [],
    },
}


def get_workspace_items(headers, workspace_id):
    """Fetch all items from the workspace."""
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return {item["displayName"]: item for item in r.json().get("value", [])}


def validate_items(ws_items):
    """Check that all expected items exist in the workspace."""
    print("\n" + "=" * 70)
    print("TASK FLOW — ITEM VALIDATION")
    print("=" * 70)

    all_expected = set()
    for task_name, task_info in TASK_ITEMS.items():
        all_expected.update(task_info["items"])

    found = 0
    missing = 0
    for item_name in sorted(all_expected):
        if item_name in ws_items:
            item = ws_items[item_name]
            print(f"  ✅ {item_name} ({item['type']})")
            found += 1
        else:
            print(f"  ❌ {item_name} — NOT FOUND")
            missing += 1

    print(f"\n  Found: {found} | Missing: {missing} | Total expected: {len(all_expected)}")
    return missing == 0


def print_assignment_guide(ws_items):
    """Print a step-by-step guide for assigning items to tasks."""
    print("\n" + "=" * 70)
    print("TASK → ITEM ASSIGNMENT GUIDE")
    print("=" * 70)
    print("After importing the task flow JSON, assign items to each task:")
    print("  1. Click a task in the task flow")
    print("  2. Click the 📎 (clip) icon to assign existing items")
    print("  3. Select the items listed below for each task\n")

    for task_name, task_info in TASK_ITEMS.items():
        items = task_info["items"]
        task_type = task_info["task_type"]
        icon = {
            "Get data": "📥",
            "Store data": "💾",
            "Visualize data": "📊",
            "Analyze and train data": "🔬",
            "Track data": "📡",
            "General": "⚙️",
        }.get(task_type, "📌")

        print(f"\n  {icon} {task_name} ({task_type})")
        if items:
            for item_name in items:
                status = "✅" if item_name in ws_items else "⚠️"
                item_type = ws_items.get(item_name, {}).get("type", "—")
                print(f"     {status} {item_name} [{item_type}]")
        else:
            print(f"     (external — configure via Power Automate / Teams)")


def main():
    print("=" * 70)
    print("🚲 Bicycle RTI — Task Flow Deployment Helper")
    print("=" * 70)

    # Authenticate
    token = get_fabric_token(TENANT_ID, ADMIN_ACCOUNT)
    headers = get_auth_headers(token)

    # Fetch workspace items
    ws_items = get_workspace_items(headers, WORKSPACE_ID)
    print(f"\nWorkspace: {WORKSPACE_ID}")
    print(f"Total items: {len(ws_items)}")

    # Validate
    all_valid = validate_items(ws_items)

    # Print assignment guide
    print_assignment_guide(ws_items)

    # Task flow JSON location
    tf_json = os.path.join(SCRIPT_DIR, "bicycle_rti_task_flow.json")

    print("\n" + "=" * 70)
    print("NEXT STEPS")
    print("=" * 70)
    print(f"""
  1. Open Fabric workspace in browser:
     https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}

  2. Switch to List View (top-left icon)

  3. In the Task Flow area at the top, click:
     "Import a task flow" (or ⋯ menu → Import)

  4. Select the file:
     {tf_json}

  5. After import, assign items to tasks using the guide above.
     (Click each task → 📎 clip icon → select items)

  6. Save the task flow.
""")

    if not all_valid:
        print("  ⚠️  Some items are missing. Deploy all items first,")
        print("     then re-run this script to verify.")

    print("Done.")


if __name__ == "__main__":
    main()
