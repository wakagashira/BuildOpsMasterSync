import requests
import time
from datetime import datetime


class BuildOpsClient:
    """
    BuildOps API client.

    Handles:
    - authentication
    - paging
    - data retrieval
    """

    def __init__(self, tenant_id):

        self.tenant_id = (tenant_id or "").strip()

        self.client_id = None
        self.client_secret = None

        self.base_url = "https://public-api.live.buildops.com/v1"

        self.token = None
        self.token_expiration = 0

        self._load_credentials()

        self.authenticate()

    # -----------------------------------------------------
    # LOAD CREDS
    # -----------------------------------------------------

    def _load_credentials(self):

        import os

        self.client_id = os.getenv("BUILDOPS_CLIENT_ID")
        self.client_secret = os.getenv("BUILDOPS_CLIENT_SECRET")

        if not self.client_id or not self.client_secret:
            raise ValueError(
                "BUILDOPS_CLIENT_ID and BUILDOPS_CLIENT_SECRET must be set"
            )

    # -----------------------------------------------------
    # AUTH
    # -----------------------------------------------------

    def authenticate(self):

        print(f"[BuildOps] Authenticating tenant {self.tenant_id}")

        auth_url = f"{self.base_url}/auth/token"

        payload = {
            "clientId": self.client_id,
            "clientSecret": self.client_secret,
            "tenantId": self.tenant_id
        }

        r = requests.post(auth_url, json=payload, timeout=30)

        if r.status_code != 200:
            raise Exception(
                f"BuildOps auth failed ({r.status_code}): {r.text}"
            )

        data = r.json()

        self.token = data["access_token"]
        self.token_expiration = time.time() + data["expires_in"]

    # -----------------------------------------------------
    # TOKEN REFRESH
    # -----------------------------------------------------

    def refresh_if_needed(self):

        if time.time() >= self.token_expiration - 60:
            self.authenticate()

    # -----------------------------------------------------
    # HEADERS
    # -----------------------------------------------------

    def _headers(self):

        self.refresh_if_needed()

        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
            "tenantId": self.tenant_id
        }

    # -----------------------------------------------------
    # TIMESTAMP HELPERS
    # -----------------------------------------------------

    @staticmethod
    def _parse_datetime(value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None

    @staticmethod
    def extract_updated_timestamp(obj):
        """
        Best-effort extraction of the most useful update timestamp from
        BuildOps payloads.

        Used for:
        - observation logging
        - client-side incremental filtering where API filters are unavailable
        """
        audit = obj.get("audit") or {}

        candidates = [
            audit.get("lastUpdatedDate"),
            obj.get("offlineUpdatedDateTime"),
            audit.get("createdDate"),
        ]

        for candidate in candidates:
            parsed = BuildOpsClient._parse_datetime(candidate)
            if parsed:
                return parsed

        return None

    # -----------------------------------------------------
    # CUSTOMERS
    # -----------------------------------------------------

    def fetch_customers_page(self, page=0, limit=100, updated_after=None):

        print(
            f"[BuildOps] Fetching CUSTOMERS page {page} "
            f"(tenant {self.tenant_id})"
        )

        url = f"{self.base_url}/customers"

        params = {
            "include_inactive": "true",
            "page": page,
            "limit": limit,
        }

        if updated_after:
            params["updatedAfter"] = updated_after.isoformat()

        resp = requests.get(url, headers=self._headers(), params=params, timeout=30)

        if resp.status_code != 200:
            raise Exception(
                f"Failed to fetch customers page {page}: "
                f"{resp.status_code} {resp.text}"
            )

        data = resp.json()

        return data.get("items", []), data.get("totalCount", 0)

    def fetch_all_customers(self, limit=100, updated_after=None):

        all_items = []
        page = 0

        while True:

            items, total = self.fetch_customers_page(
                page=page,
                limit=limit,
                updated_after=updated_after
            )

            all_items.extend(items)

            if len(items) < limit:
                break

            page += 1

        return all_items

    # -----------------------------------------------------
    # EMPLOYEES
    # -----------------------------------------------------

    def fetch_employees_page(self, page=0, limit=100):

        print(
            f"[BuildOps] Fetching EMPLOYEES page {page} "
            f"(tenant {self.tenant_id})"
        )

        url = f"{self.base_url}/employees"

        params = {
            "include_inactive": "true",
            "page": page,
            "page_size": limit,
        }

        resp = requests.get(url, headers=self._headers(), params=params, timeout=30)

        if resp.status_code != 200:
            raise Exception(
                f"Failed to fetch employees page {page}: "
                f"{resp.status_code} {resp.text}"
            )

        data = resp.json()

        return data.get("items", []), data.get("totalCount", 0)

    def fetch_all_employees(self, limit=100, updated_after=None):
        """
        Employees endpoint does not document updatedAfter.
        We fetch all pages and optionally filter client-side.
        """

        all_items = []
        page = 0

        while True:

            items, total = self.fetch_employees_page(page=page, limit=limit)

            all_items.extend(items)

            if len(items) < limit:
                break

            page += 1

        if not updated_after:
            return all_items

        filtered_items = []

        for item in all_items:
            ts = self.extract_updated_timestamp(item)
            if ts and ts >= updated_after:
                filtered_items.append(item)

        return filtered_items

    # -----------------------------------------------------
    # JOBS
    # -----------------------------------------------------

    def fetch_jobs_page(self, page=0, limit=100, updated_after=None):

        print(
            f"[BuildOps] Fetching JOBS page {page} "
            f"(tenant {self.tenant_id})"
        )

        url = f"{self.base_url}/jobs"

        params = {
            "page": page,
            "page_size": limit,
        }

        if updated_after:
            params["updatedAfter"] = updated_after.isoformat()

        resp = requests.get(url, headers=self._headers(), params=params, timeout=30)

        if resp.status_code != 200:
            raise Exception(
                f"Failed to fetch jobs page {page}: "
                f"{resp.status_code} {resp.text}"
            )

        data = resp.json()

        return data.get("items", []), data.get("totalCount", 0)

    def fetch_all_jobs(self, limit=100, updated_after=None):

        all_items = []
        page = 0

        while True:

            items, total = self.fetch_jobs_page(
                page=page,
                limit=limit,
                updated_after=updated_after
            )

            all_items.extend(items)

            if len(items) < limit:
                break

            page += 1

        return all_items

    # -----------------------------------------------------
    # QUOTES
    # -----------------------------------------------------

    def fetch_quotes_page(self, page=0, limit=100):

        print(
            f"[BuildOps] Fetching QUOTES page {page} "
            f"(tenant {self.tenant_id})"
        )

        url = f"{self.base_url}/quotes"

        params = {
            "page": page,
            "page_size": limit,
        }

        resp = requests.get(url, headers=self._headers(), params=params, timeout=30)

        if resp.status_code != 200:
            raise Exception(
                f"Failed to fetch quotes page {page}: "
                f"{resp.status_code} {resp.text}"
            )

        data = resp.json()

        return data.get("items", []), data.get("totalCount", 0)

    def fetch_all_quotes(self, limit=100):

        all_items = []
        page = 0

        while True:

            items, total = self.fetch_quotes_page(page=page, limit=limit)

            all_items.extend(items)

            if len(items) < limit:
                break

            page += 1

        return all_items