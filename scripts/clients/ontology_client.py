"""
Fabric Ontology REST API Client
================================
Reusable wrapper for the Microsoft Fabric Ontology REST API (Preview).

Endpoints covered:
  GET    /workspaces/{id}/ontologies                         — list
  GET    /workspaces/{id}/ontologies/{id}                    — get
  POST   /workspaces/{id}/ontologies                         — create
  POST   /workspaces/{id}/ontologies/{id}/getDefinition      — get definition
  POST   /workspaces/{id}/ontologies/{id}/updateDefinition   — update definition
  DELETE /workspaces/{id}/ontologies/{id}                    — delete

Based on: https://learn.microsoft.com/en-us/rest/api/fabric/ontology/items
Reference: https://github.com/UnifiedEducation/research/tree/main/ontology

Usage:
    from clients.ontology_client import OntologyClient
    client = OntologyClient(token)
    client.create(ws_id, "My_Ontology", "Description", parts)
"""
from __future__ import annotations

import json
import time
import base64
import requests

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


class OntologyClient:
    """Thin wrapper around the Fabric Ontology REST API."""

    def __init__(self, token: str, api_base: str = FABRIC_API_BASE):
        self.token = token
        self.api_base = api_base
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    # ── List / Get / Find ─────────────────────────────────────

    def list(self, workspace_id: str) -> list[dict]:
        """List all ontologies in a workspace."""
        url = f"{self.api_base}/workspaces/{workspace_id}/ontologies"
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json().get("value", [])
        # Fallback: generic items endpoint
        url = f"{self.api_base}/workspaces/{workspace_id}/items?type=Ontology"
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json().get("value", [])
        return []

    def get(self, workspace_id: str, ontology_id: str) -> dict | None:
        """Get a single ontology by ID."""
        url = f"{self.api_base}/workspaces/{workspace_id}/ontologies/{ontology_id}"
        resp = requests.get(url, headers=self._headers)
        if resp.status_code == 200:
            return resp.json()
        return None

    def find_by_name(self, workspace_id: str, name: str) -> dict | None:
        """Find an ontology by display name."""
        for item in self.list(workspace_id):
            if item.get("displayName") == name:
                return item
        return None

    # ── Create ────────────────────────────────────────────────

    def create(self, workspace_id: str, display_name: str, description: str,
               parts: list[dict] = None, folder_id: str = None) -> str | None:
        """
        Create an ontology with optional initial definition.

        Returns:
            Ontology item ID on success, None on failure.
        """
        url = f"{self.api_base}/workspaces/{workspace_id}/ontologies"
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
            print(f"  [OK] Created ontology: {display_name} (ID: {item_id})")
            return item_id

        if resp.status_code == 202:
            ok = self._wait_lro(resp, display_name)
            if ok:
                item = self.find_by_name(workspace_id, display_name)
                if item:
                    print(f"  [OK] Created ontology: {display_name} (ID: {item['id']})")
                    return item["id"]
            print(f"  [FAIL] Async create failed: {display_name}")
            return None

        if resp.status_code == 409 or "AlreadyInUse" in resp.text:
            print(f"  [WARN] Already exists: {display_name}")
            item = self.find_by_name(workspace_id, display_name)
            return item["id"] if item else None

        print(f"  [FAIL] Create ontology: HTTP {resp.status_code}")
        print(f"    {resp.text[:500]}")
        return None

    # ── Get Definition ────────────────────────────────────────

    def get_definition(self, workspace_id: str, ontology_id: str) -> dict | None:
        """
        Get the ontology definition (base64-encoded parts).
        Handles LRO (202).
        """
        url = (f"{self.api_base}/workspaces/{workspace_id}"
               f"/ontologies/{ontology_id}/getDefinition")
        resp = requests.post(url, headers=self._headers)

        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 202:
            return self._handle_lro_result(resp)

        print(f"  [FAIL] getDefinition: HTTP {resp.status_code}")
        return None

    def get_definition_decoded(self, workspace_id: str, ontology_id: str) -> dict:
        """Get definition with payloads decoded from base64 to dicts."""
        raw = self.get_definition(workspace_id, ontology_id)
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

    def update_definition(self, workspace_id: str, ontology_id: str,
                          parts: list[dict]) -> bool:
        """
        Push updated definition parts to an existing ontology.
        Handles LRO (202).
        """
        url = (f"{self.api_base}/workspaces/{workspace_id}"
               f"/ontologies/{ontology_id}/updateDefinition")
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

    # ── Delete ────────────────────────────────────────────────

    def delete(self, workspace_id: str, ontology_id: str) -> bool:
        """Delete an ontology."""
        url = f"{self.api_base}/workspaces/{workspace_id}/ontologies/{ontology_id}"
        resp = requests.delete(url, headers=self._headers)
        if resp.status_code in (200, 204):
            print(f"  [OK] Deleted ontology: {ontology_id}")
            return True
        print(f"  [FAIL] Delete: HTTP {resp.status_code} — {resp.text[:300]}")
        return False

    # ── Internal helpers ──────────────────────────────────────

    def _wait_lro(self, response, label: str, timeout: int = 180) -> bool:
        """Poll a long-running operation until completion."""
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
        """Poll LRO and fetch the /result endpoint."""
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
