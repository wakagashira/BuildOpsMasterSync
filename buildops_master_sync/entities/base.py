from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime

from buildops_master_sync.connectors.buildops_client import BuildOpsClient


class SyncEntity(ABC):
    name: str

    @abstractmethod
    def fetch(self, client: BuildOpsClient, since: Optional[datetime]) -> List[Dict[str, Any]]:
        ...

    @abstractmethod
    def upsert(self, records: List[Dict[str, Any]], tenant_id: str) -> int:
        ...
