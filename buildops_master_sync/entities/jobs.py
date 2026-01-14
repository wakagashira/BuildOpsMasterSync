from datetime import datetime

from buildops_master_sync.connectors.sql import get_connection
from buildops_master_sync.connectors.buildops_client import BuildOpsClient


def safe_dt(value):
    try:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


class JobsEntity:
    name = "Jobs"

    def sync(self, tenant_id, since=None):
        client = BuildOpsClient(tenant_id=tenant_id)
        jobs = client.fetch_all_jobs(updated_after=since)

        rows_fetched = len(jobs)
        if rows_fetched == 0:
            print("[OBSERVE] Jobs: no rows fetched")
            return 0, 0

        # ------------------------------------------------------------
        # Phase 1: Incremental correctness observation
        # ------------------------------------------------------------
        max_seen_updated_at = None

        for job in jobs:
            ts = client.extract_updated_timestamp(job)
            if ts and (not max_seen_updated_at or ts > max_seen_updated_at):
                max_seen_updated_at = ts

        print(
            f"[OBSERVE] Jobs max updated timestamp: "
            f"{max_seen_updated_at}"
        )

        rows_merged = self._upsert(jobs, tenant_id)
        return rows_fetched, rows_merged

    def _upsert(self, jobs, tenant_id):
        conn = get_connection()
        cursor = conn.cursor()

        upsert_sql = """
        MERGE dbo.Jobs AS tgt
        USING (VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )) AS src (
            JobId,
            JobNumber,
            Status,
            IssueDescription,
            CustomerId,
            CustomerName,
            CustomerPropertyName,
            CustomerRepName,
            JobTypeName,
            Priority,
            CostAmount,
            AmountQuoted,
            DueDate,
            CompletedDate,
            TenantId
        )
        ON tgt.JobId = src.JobId

        WHEN MATCHED THEN UPDATE SET
            JobNumber = src.JobNumber,
            Status = src.Status,
            IssueDescription = src.IssueDescription,
            CustomerId = src.CustomerId,
            CustomerName = src.CustomerName,
            CustomerPropertyName = src.CustomerPropertyName,
            CustomerRepName = src.CustomerRepName,
            JobTypeName = src.JobTypeName,
            Priority = src.Priority,
            CostAmount = src.CostAmount,
            AmountQuoted = src.AmountQuoted,
            DueDate = src.DueDate,
            CompletedDate = src.CompletedDate,
            TenantId = src.TenantId,
            LastUpdatedDate = GETDATE()

        WHEN NOT MATCHED THEN INSERT (
            JobId,
            JobNumber,
            Status,
            IssueDescription,
            CustomerId,
            CustomerName,
            CustomerPropertyName,
            CustomerRepName,
            JobTypeName,
            Priority,
            CostAmount,
            AmountQuoted,
            DueDate,
            CompletedDate,
            CreatedDate,
            LastUpdatedDate,
            TenantId
        )
        VALUES (
            src.JobId,
            src.JobNumber,
            src.Status,
            src.IssueDescription,
            src.CustomerId,
            src.CustomerName,
            src.CustomerPropertyName,
            src.CustomerRepName,
            src.JobTypeName,
            src.Priority,
            src.CostAmount,
            src.AmountQuoted,
            src.DueDate,
            src.CompletedDate,
            GETDATE(),
            GETDATE(),
            src.TenantId
        );
        """

        merged = 0

        for job in jobs:
            cursor.execute(
                upsert_sql,
                (
                    job.get("id"),
                    job.get("jobNumber"),
                    job.get("status"),
                    job.get("issueDescription"),
                    job.get("customerId"),
                    job.get("customerName"),
                    job.get("customerPropertyName"),
                    job.get("customerRepName"),
                    job.get("jobTypeName"),
                    job.get("priority"),
                    job.get("costAmount"),
                    job.get("amountQuoted"),
                    safe_dt(job.get("dueDate")),
                    safe_dt(job.get("completedDate")),
                    tenant_id,
                ),
            )
            merged += 1

        conn.commit()
        conn.close()
        return merged
