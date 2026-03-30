"""
Fabric Graph Model REST API Client
====================================
Reusable wrapper for the Microsoft Fabric GraphModel REST API (Preview).

Endpoints covered:
  GET    /workspaces/{id}/graphModels                               — list
  GET    /workspaces/{id}/graphModels/{id}                          — get
  POST   /workspaces/{id}/graphModels                               — create
  POST   /workspaces/{id}/graphModels/{id}/getDefinition            — get definition
  POST   /workspaces/{id}/graphModels/{id}/updateDefinition         — update definition
  POST   /workspaces/{id}/graphModels/{id}/jobs/refreshGraph/instances — refresh
  GET    /workspaces/{id}/graphModels/{id}/getQueryableGraphType    — schema (beta)
  POST   /workspaces/{id}/graphModels/{id}/executeQuery             — GQL query (beta)
  DELETE /workspaces/{id}/graphModels/{id}                          — delete

Based on: https://learn.microsoft.com/en-us/rest/api/fabric/graphmodel/items
Reference: https://github.com/UnifiedEducation/research/tree/main/ontology

NOTE: Graph refresh for ontology-managed (auto-provisioned) graphs returns
      InvalidJobType. Only the Fabric UI 'Refresh now' button works.
      Standalone graph models CAN be refreshed via API.
"""
from __future__ import annotations

import json
import time
import base64
import requests

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


class GraphModelClient:
    """Thin wrapper around the Fabric GraphModel REST API."""

    def __init__(self, token: str, api_base: str = FABRIC_API_BASE):
        self.token = token
        self.api_base = api_base
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    # ── List / Get / Find ─────────────────────────────────────

    def list(self, workspace_id: str) -> list[dict]:
        """List all graph models in a workspace."""
        url = f"{self.api_base}/workspaces/{workspace_id}/graphModels"
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json().get("value", [])
        # Fallback to items endpoint
        url = f"{self.api_base}/workspaces/{workspace_id}/items?type=GraphModel"
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json().get("value", [])
        return []

    def get(self, workspace_id: str, graph_id: str) -> dict | None:
        """Get a single graph model by ID."""
        url = f"{self.api_base}/workspaces/{workspace_id}/graphModels/{graph_id}"
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json()
        return None

    def find_by_name(self, workspace_id: str, name: str) -> dict | None:
        """Find a graph model by display name."""
        for item in self.list(workspace_id):
            if item.get("displayName") == name:
                return item
        return None

    # ── Create ────────────────────────────────────────────────

    def create(self, workspace_id: str, display_name: str, description: str,
               parts: list[dict] = None, folder_id: str = None) -> str | None:
        """Create a graph model with optional definition."""
        url = f"{self.api_base}/workspaces/{workspace_id}/graphModels"
        body: dict = {
            "displayName": display_name,
            "description": description,
        }
        if parts:
            body["definition"] = {"parts": parts}
        if folder_id:
            body["folderId"] = folder_id

        resp = requests.post(url, headers=self._headers, json=body)

        if resp.status_code in (200, 201):
            item_id = resp.json().get("id")
            print(f"  [OK] Created graph model: {display_name} (ID: {item_id})")
            return item_id

        if resp.status_code == 202:
            ok = self._wait_lro(resp, display_name)
            if ok:
                item = self.find_by_name(workspace_id, display_name)
                if item:
                    print(f"  [OK] Created graph model: {display_name} (ID: {item['id']})")
                    return item["id"]
            return None

        if resp.status_code == 409 or "AlreadyInUse" in resp.text:
            print(f"  [WARN] Already exists: {display_name}")
            item = self.find_by_name(workspace_id, display_name)
            return item["id"] if item else None

        print(f"  [FAIL] Create graph model: HTTP {resp.status_code}")
        print(f"    {resp.text[:500]}")
        return None

    # ── Get Definition ────────────────────────────────────────

    def get_definition(self, workspace_id: str, graph_id: str) -> dict | None:
        """Get graph model definition (base64 parts). Handles LRO."""
        url = (f"{self.api_base}/workspaces/{workspace_id}"
               f"/graphModels/{graph_id}/getDefinition")
        resp = requests.post(url, headers=self._headers)

        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 202:
            return self._handle_lro_result(resp)
        return None

    def get_definition_decoded(self, workspace_id: str, graph_id: str) -> dict:
        """Get definition with payloads decoded from base64."""
        raw = self.get_definition(workspace_id, graph_id)
        if not raw:
            return {}
        parts = raw.get("definition", {}).get("parts", [])
        decoded = {}
        for part in parts:
            payload = part.get("payload", "")
            try:
                decoded[part["path"]] = json.loads(base64.b64decode(payload))
            except (json.JSONDecodeError, Exception):
                decoded[part["path"]] = base64.b64decode(payload).decode(
                    "utf-8", errors="replace"
                )
        return decoded

    # ── Update Definition ─────────────────────────────────────

    def update_definition(self, workspace_id: str, graph_id: str,
                          parts: list[dict]) -> bool:
        """Push updated definition. Handles LRO."""
        url = (f"{self.api_base}/workspaces/{workspace_id}"
               f"/graphModels/{graph_id}/updateDefinition")
        body = {"definition": {"parts": parts}}
        resp = requests.post(url, headers=self._headers, json=body)

        if resp.status_code in (200, 201):
            print(f"  [OK] Definition updated")
            return True
        if resp.status_code == 202:
            ok = self._wait_lro(resp, "updateDefinition")
            if ok:
                print(f"  [OK] Definition updated")
            return ok

        print(f"  [FAIL] updateDefinition: HTTP {resp.status_code}")
        print(f"    {resp.text[:500]}")
        return False

    # ── Refresh ───────────────────────────────────────────────

    def refresh(self, workspace_id: str, graph_id: str,
                wait: bool = True, timeout: int = 600) -> bool:
        """
        Trigger an on-demand graph refresh.

        NOTE: This does NOT work for ontology-managed (auto-provisioned) graphs.
              Those return InvalidJobType. Use Fabric UI 'Refresh now' instead.
              Standalone graph models CAN be refreshed via this API.
        """
        url = (f"{self.api_base}/workspaces/{workspace_id}"
               f"/graphModels/{graph_id}/jobs/refreshGraph/instances")
        resp = requests.post(url, headers=self._headers)

        if resp.status_code == 200:
            print("  [OK] Refresh completed immediately")
            return True

        if resp.status_code == 202:
            if not wait:
                print("  [OK] Refresh job accepted")
                return True
            location = resp.headers.get("Location")
            retry = int(resp.headers.get("Retry-After", 15))
            print(f"  Refresh job accepted (polling every {retry}s)...")
            start = time.time()
            while time.time() - start < timeout:
                time.sleep(retry)
                poll = requests.get(location, headers=self._headers)
                if poll.status_code == 200:
                    body = poll.json()
                    status = body.get("status", "Unknown")
                    print(f"    Refresh status: {status}")
                    if status in ("Completed", "Succeeded"):
                        return True
                    if status in ("Failed", "Cancelled"):
                        reason = body.get("failureReason", {})
                        print(f"    [FAIL] {reason.get('message', body)}")
                        return False
            print(f"    [FAIL] Refresh timed out after {timeout}s")
            return False

        # Common failure: InvalidJobType for ontology-managed graphs
        if resp.status_code in (400, 404):
            error = resp.json().get("error", {}) if resp.text else {}
            code = error.get("errorCode", error.get("code", ""))
            if "InvalidJobType" in code or "InvalidJobType" in resp.text:
                print("  [WARN] InvalidJobType — this is an ontology-managed graph.")
                print("         API refresh is not supported; use Fabric UI 'Refresh now'.")
                return False

        print(f"  [FAIL] Refresh: HTTP {resp.status_code}")
        print(f"    {resp.text[:300]}")
        return False

    # ── Query (beta) ──────────────────────────────────────────

    def execute_query(self, workspace_id: str, graph_id: str, query: str) -> dict | None:
        """Execute a GQL query against the graph model (beta)."""
        url = (f"{self.api_base}/workspaces/{workspace_id}"
               f"/graphModels/{graph_id}/executeQuery")
        resp = requests.post(
            url, headers=self._headers,
            params={"beta": "true"},
            json={"query": query},
        )
        if resp.status_code == 200:
            return resp.json()
        return None

    def get_queryable_graph_type(self, workspace_id: str, graph_id: str) -> dict | None:
        """Get the graph schema — node types and edge types (beta)."""
        url = (f"{self.api_base}/workspaces/{workspace_id}"
               f"/graphModels/{graph_id}/getQueryableGraphType")
        resp = requests.get(
            url, headers=self._headers,
            params={"beta": "true"},
        )
        if resp.status_code == 200:
            return resp.json()
        return None

    # ── Wait for Data Load ────────────────────────────────────

    def wait_for_data_load(self, workspace_id: str, graph_id: str,
                           timeout: int = 600, poll_interval: int = 30) -> bool:
        """
        Poll until graph model finishes loading data.

        After creating a graph model with a definition, Fabric auto-triggers
        a data load. This method polls GET /graphModels/{id} until
        queryReadiness == "Full" and loadingStatus == "Completed".
        """
        start = time.time()
        while time.time() - start < timeout:
            item = self.get(workspace_id, graph_id)
            if not item:
                time.sleep(poll_interval)
                continue
            props = item.get("properties", {})
            readiness = props.get("queryReadiness", "")
            loading = props.get("lastDataLoadingStatus") or {}
            status = loading.get("status", "")
            print(f"    queryReadiness={readiness}  loadingStatus={status}")
            if readiness == "Full" and status == "Completed":
                return True
            if status == "Failed":
                print(f"    [WARN] Data load failed")
                return False
            time.sleep(poll_interval)
        print(f"    [WARN] Timed out waiting for data load ({timeout}s)")
        return False

    # ── Delete ────────────────────────────────────────────────

    def delete(self, workspace_id: str, graph_id: str) -> bool:
        """Delete a graph model."""
        url = f"{self.api_base}/workspaces/{workspace_id}/graphModels/{graph_id}"
        resp = requests.delete(url, headers=self._headers)
        if resp.status_code in (200, 204):
            print(f"  [OK] Deleted graph model: {graph_id}")
            return True
        print(f"  [FAIL] Delete: HTTP {resp.status_code} — {resp.text[:300]}")
        return False

    # ── Internal helpers ──────────────────────────────────────

    def _wait_lro(self, response, label: str, timeout: int = 180) -> bool:
        """Poll a long-running operation."""
        location = response.headers.get("Location")
        if not location:
            time.sleep(10)
            return True
        start = time.time()
        retry = int(response.headers.get("Retry-After", 5))
        while time.time() - start < timeout:
            time.sleep(retry)
            r = requests.get(location, headers=self._headers)
            if r.status_code == 200:
                status = r.json().get("status", "")
                if status == "Succeeded":
                    return True
                if status in ("Failed", "Cancelled"):
                    err = r.json().get("error", {}).get("message", "")
                    print(f"    [FAIL] {label}: {status} — {err}")
                    return False
            elif r.status_code == 404:
                time.sleep(3)
                return True
        print(f"    [FAIL] {label}: timed out after {timeout}s")
        return False

    def _handle_lro_result(self, response) -> dict | None:
        """Poll LRO and fetch /result endpoint."""
        location = response.headers.get("Location")
        if not location:
            return None
        retry = int(response.headers.get("Retry-After", 5))
        for _ in range(60):
            time.sleep(retry)
            poll = requests.get(location, headers=self._headers)
            if poll.status_code == 200:
                status = poll.json().get("status", "")
                if status == "Succeeded":
                    result = requests.get(f"{location}/result", headers=self._headers)
                    if result.status_code == 200:
                        return result.json()
                    return poll.json()
                if status in ("Failed", "Cancelled"):
                    return None
        return None

    @staticmethod
    def encode_part(path: str, content) -> dict:
        """Encode a definition part for API calls."""
        if isinstance(content, dict):
            content = json.dumps(content, ensure_ascii=False)
        elif isinstance(content, bytes):
            content = content.decode("utf-8")
        payload = base64.b64encode(content.encode("utf-8")).decode("ascii")
        return {"path": path, "payload": payload, "payloadType": "InlineBase64"}
