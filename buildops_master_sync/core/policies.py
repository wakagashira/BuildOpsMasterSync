import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional


@dataclass(frozen=True)
class SyncPolicy:
    sync_mode: str  # FULL or INCREMENTAL
    skew_minutes: int = 5

    @staticmethod
    def from_env() -> "SyncPolicy":
        mode = (os.getenv("SYNC_MODE") or "full").strip().lower()
        sync_mode = "INCREMENTAL" if mode == "incremental" else "FULL"
        skew = int(os.getenv("SYNC_SKEW_MINUTES") or "5")
        return SyncPolicy(sync_mode=sync_mode, skew_minutes=skew)

    def cutoff(self, last_synced_at: Optional[datetime]) -> Optional[datetime]:
        if self.sync_mode != "INCREMENTAL":
            return None
        if not last_synced_at:
            return None
        return last_synced_at - timedelta(minutes=self.skew_minutes)
