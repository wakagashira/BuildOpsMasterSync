import os
import socket
from datetime import datetime, timedelta

from buildops_master_sync.connectors.sql import get_connection
from buildops_master_sync.state.tenants import (
    load_active_tenants,
    update_tenant_success,
    update_tenant_failure,
)
from buildops_master_sync.state.runs import (
    create_sync_run,
    finish_sync_run,
    start_entity_run,
    finish_entity_run,
)
from buildops_master_sync.entities.customers import CustomersEntity
from buildops_master_sync.entities.employees import EmployeesEntity
from buildops_master_sync.entities.jobs import JobsEntity
from buildops_master_sync.entities.quotes import QuotesEntity


class MasterSyncEngine:
    def __init__(self):
        self.sync_mode = os.getenv("SYNC_MODE", "full").lower()
        self.skew_minutes = int(os.getenv("SYNC_SKEW_MINUTES", "5"))

        self.hostname = socket.gethostname()
        self.started_at = datetime.utcnow()

        # Sync order matters
        self.entities = [
            CustomersEntity(),
            EmployeesEntity(),
            JobsEntity(),
            QuotesEntity(),
        ]

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self):
        print("=== Build Ops Master Sync starting ===")
        print(f"Sync mode: {self.sync_mode}")

        conn = get_connection()
        cursor = conn.cursor()

        run_id = create_sync_run(
            cursor=cursor,
            sync_mode=self.sync_mode,
            initiated_by=self.hostname,
        )
        conn.commit()

        overall_status = "SUCCESS"

        try:
            tenants = load_active_tenants(cursor)
            print(f"Tenants discovered: {len(tenants)}")

            for tenant in tenants:
                tenant_id = tenant["Tenant"]
                last_synced_at = tenant.get("last_synced_at")

                cutoff = self._compute_cutoff(last_synced_at)

                print(f"\n--- Tenant {tenant_id} --- (cutoff={cutoff})")

                try:
                    self._run_tenant(
                        cursor=cursor,
                        run_id=run_id,
                        tenant_id=tenant_id,
                        cutoff=cutoff,
                    )
                    update_tenant_success(cursor, tenant_id)

                except PermissionError as auth_error:
                    overall_status = "PARTIAL"
                    print(f"[SKIP] Unauthorized tenant {tenant_id}: {auth_error}")
                    update_tenant_failure(
                        cursor,
                        tenant_id,
                        str(auth_error),
                    )

                except Exception as tenant_error:
                    overall_status = "PARTIAL"
                    print(f"✗ Tenant failed {tenant_id}: {tenant_error}")
                    update_tenant_failure(
                        cursor,
                        tenant_id,
                        str(tenant_error),
                    )

                conn.commit()

        except Exception as fatal_error:
            overall_status = "FAILED"
            finish_sync_run(
                cursor,
                run_id,
                status="FAILED",
                error_summary=str(fatal_error),
            )
            conn.commit()
            conn.close()
            raise

        finish_sync_run(cursor, run_id, status=overall_status)
        conn.commit()
        conn.close()

        print(f"=== Sync completed ({overall_status}) ===")

    # ------------------------------------------------------------------
    # Tenant execution
    # ------------------------------------------------------------------

    def _run_tenant(self, cursor, run_id, tenant_id, cutoff):
        for entity in self.entities:
            entity_name = entity.name
            print(f"→ Syncing {entity_name}")

            entity_run_id = start_entity_run(
                cursor,
                run_id=run_id,
                tenant_id=tenant_id,
                entity_name=entity_name,
            )

            try:
                rows_fetched, rows_merged = entity.sync(
                    tenant_id=tenant_id,
                    since=cutoff,
                )

                finish_entity_run(
                    cursor,
                    entity_run_id=entity_run_id,
                    status="SUCCESS",
                    rows_fetched=rows_fetched,
                    rows_merged=rows_merged,
                )

                print(
                    f"✓ {entity_name} complete "
                    f"(fetched={rows_fetched}, merged={rows_merged})"
                )

            except NotImplementedError:
                finish_entity_run(
                    cursor,
                    entity_run_id=entity_run_id,
                    status="SUCCESS",
                    rows_fetched=0,
                    rows_merged=0,
                )

                print(f"✓ {entity_name} skipped (not implemented)")

            except Exception as e:
                finish_entity_run(
                    cursor,
                    entity_run_id=entity_run_id,
                    status="FAILED",
                    error_message=str(e),
                )
                raise

    # ------------------------------------------------------------------
    # Sync policy
    # ------------------------------------------------------------------

    def _compute_cutoff(self, last_synced_at):
        if self.sync_mode != "incremental":
            return None

        if not last_synced_at:
            return None

        return last_synced_at - timedelta(minutes=self.skew_minutes)