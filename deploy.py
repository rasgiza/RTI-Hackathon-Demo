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

def main():
    # Resolve workspace/ directory relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    workspace_dir = os.path.join(script_dir, "workspace")

    if not os.path.isdir(workspace_dir):
        print(f"❌ Cannot find workspace/ folder at: {workspace_dir}")
        sys.exit(1)

    item_count = len([d for d in os.listdir(workspace_dir)
                      if os.path.isdir(os.path.join(workspace_dir, d))])
    print(f"📂 Found {item_count} items in workspace/")

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

    # Deploy in 5 stages (order matters for dependencies)
    stages = [
        ("Stage 1/5: Storage (Lakehouses)",
         ["Lakehouse"]),
        ("Stage 2/5: Real-Time (Eventhouse + KQL Database)",
         ["Eventhouse", "KQLDatabase"]),
        ("Stage 3/5: Compute & Ingestion (Notebooks, Eventstreams)",
         ["Notebook", "Eventstream"]),
        ("Stage 4/5: Analytics (Semantic Models, Pipeline)",
         ["SemanticModel", "DataPipeline"]),
        ("Stage 5/5: Presentation & AI (Report, Dashboard, Agents, Activators)",
         ["Report", "KQLDashboard", "DataAgent", "Reflex"]),
    ]

    for label, item_types in stages:
        print(f"\n{'='*60}")
        print(f"🚀 {label}")
        print(f"{'='*60}")

        kwargs = {"repository_directory": workspace_dir,
                  "item_type_in_scope": item_types,
                  "token_credential": credential}
        if ws_id:
            kwargs["workspace_id"] = ws_id
        else:
            kwargs["workspace_name"] = ws_name

        ws = FabricWorkspace(**kwargs)
        publish_all_items(ws)
        print(f"   ✅ {label.split(':')[0]} complete")

    print(f"\n{'='*60}")
    print("✅ ALL 26 ITEMS DEPLOYED SUCCESSFULLY")
    print(f"{'='*60}")
    print()
    print("Next steps:")
    print("  1. Upload Post_Deploy_Setup.ipynb to your workspace → Run all cells")
    print("     (creates Ontology, Graph Model, Operations Agent)")
    print("  2. Open PL_BicycleRTI_Medallion pipeline → click Run")
    print("     (first load takes ~15-25 min)")
    print("  3. Open Graph Model → click 'Refresh now'")
    print("  4. Verify Eventstreams are started")
    print("  5. Test the Data Agent: ask 'Which stations need rebalancing?'")


if __name__ == "__main__":
    main()
