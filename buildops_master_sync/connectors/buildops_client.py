"""
BuildOps API Client

Responsibilities:
- Authenticate with BuildOps Public API
- Handle token refresh
- Fetch paginated resources
- Support incremental filters via updatedAfter
- Expose update timestamps for validation
"""

import os
import time
import requests
from datetime import datetime


class BuildOpsClient:
    BASE_URL = "https://public-api.live.buildops.com/v1"

    def __init__(self, tenant_id):
        self.client_id = os.getenv("BUILDOPS_CLIENT_ID")
        self.client_secret = os.getenv("BUILDOPS_CLIENT_SECRET")
        self.tenant_id = tenant_id

        if not self.client_id or not self.client_secret:
            raise ValueError(
                "BUILDOPS_CLIENT_ID and BUILDOPS_CLIENT_SECRET must be set"
            )

        self.token = None
        self.token_expires_at = 0
        self._authenticate()

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _authenticate(self):
        url = f"{self.BASE_URL}/auth/token"
        payload = {
            "clientId": self.client_id,
            "clientSecret": self.client_secret,
            "tenantId": self.tenant_id,
        }

        print(f"[BuildOps] Authenticating tenant {self.tenant_id}")
        resp = requests.post(url, json=payload, timeout=30)

        if resp.status_code != 200:
            raise RuntimeError(
                f"BuildOps auth failed ({resp.status_code}): {resp.text}"
            )

        data = resp.json()
        self.token = data["access_token"]
        self.token_expires_at = time.time() + data["expires_in"]

    def _refresh_if_needed(self):
        if time.time() >= self.token_expires_at - 60:
            self._authenticate()

    def _headers(self):
        self._refresh_if_needed()
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "tenantId": self.tenant_id,
        }

    # ------------------------------------------------------------------
    # Timestamp helpers (NEW)
    # ------------------------------------------------------------------

    @staticmethod
    def extract_updated_timestamp(obj):
        """
        Best-effort extraction of update timestamp from BuildOps payload.
        """
        audit = obj.get("audit") or {}
        ts = (
            audit.get("lastUpdatedDate")
            or audit.get("lastUpdatedDateTime")
            or audit.get("createdDate")
        )

        if not ts:
            return None

        try:
            return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Customers
    # ------------------------------------------------------------------

    def fetch_all_customers(self, limit=100, updated_after=None):
        all_items = []
        page = 0

        while True:
            print(
                f"[BuildOps] Fetching CUSTOMERS page {page} "
                f"(tenant {self.tenant_id})"
            )

            params = {
                "page": page,
                "limit": limit,
                "include_inactive": "true",
            }

            if updated_after:
                params["updatedAfter"] = updated_after.isoformat()

            resp = requests.get(
                f"{self.BASE_URL}/customers",
                headers=self._headers(),
                params=params,
                timeout=30,
            )

            if resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch customers (page {page}): "
                    f"{resp.status_code} {resp.text}"
                )

            data = resp.json()
            items = data.get("items", [])
            all_items.extend(items)

            if len(items) < limit:
                break

            page += 1

        return all_items

    # ------------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------------

    def fetch_all_jobs(self, limit=100, updated_after=None):
        all_items = []
        page = 0

        while True:
            print(
                f"[BuildOps] Fetching JOBS page {page} "
                f"(tenant {self.tenant_id})"
            )

            params = {
                "page": page,
                "page_size": limit,
            }

            if updated_after:
                params["updatedAfter"] = updated_after.isoformat()

            resp = requests.get(
                f"{self.BASE_URL}/jobs",
                headers=self._headers(),
                params=params,
                timeout=30,
            )

            if resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch jobs (page {page}): "
                    f"{resp.status_code} {resp.text}"
                )

            data = resp.json()
            items = data.get("items", [])
            all_items.extend(items)

            if len(items) < limit:
                break

            page += 1

        return all_items
