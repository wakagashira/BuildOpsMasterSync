from datetime import datetime
from decimal import Decimal, InvalidOperation

from buildops_master_sync.connectors.sql import get_connection
from buildops_master_sync.connectors.buildops_client import BuildOpsClient


def safe_dt(value):
    try:
        if not value:
            return None
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def safe_decimal(value):
    try:
        if value is None or value == "":
            return None
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def safe_str(value, max_len=None):
    if value is None:
        return None
    s = str(value)
    if max_len is not None:
        return s[:max_len]
    return s


class JobsEntity:
    name = "Jobs"

    def sync(self, tenant_id, since=None):
        client = BuildOpsClient(tenant_id=tenant_id)
        jobs = client.fetch_all_jobs(updated_after=since)

        rows_fetched = len(jobs)
        if rows_fetched == 0:
            print("[OBSERVE] Jobs: no rows fetched")
            return 0, 0

        max_seen_updated_at = None
        for job in jobs:
            ts = client.extract_updated_timestamp(job)
            if ts and (not max_seen_updated_at or ts > max_seen_updated_at):
                max_seen_updated_at = ts

        print(
            f"[OBSERVE] Jobs max updated timestamp: "
            f"{max_seen_updated_at}"
        )

        deduped_jobs, duplicate_count = self._dedupe_jobs(jobs, client)

        if duplicate_count > 0:
            print(
                f"[OBSERVE] Jobs duplicates removed: {duplicate_count} "
                f"(kept {len(deduped_jobs)} unique ids out of {len(jobs)})"
            )

        rows_merged = self._stage_and_merge(deduped_jobs, tenant_id)
        return rows_fetched, rows_merged

    def _dedupe_jobs(self, jobs, client):
        """
        Deduplicate jobs by id, keeping the record with the latest
        observed updated timestamp. If timestamps are equal or missing,
        keep the later occurrence.
        """
        by_id = {}
        duplicate_count = 0

        for job in jobs:
            job_id = safe_str(job.get("id"), 50)

            if not job_id:
                continue

            current_ts = client.extract_updated_timestamp(job)

            if job_id not in by_id:
                by_id[job_id] = (job, current_ts)
                continue

            duplicate_count += 1
            existing_job, existing_ts = by_id[job_id]

            if existing_ts is None and current_ts is not None:
                by_id[job_id] = (job, current_ts)
            elif existing_ts is not None and current_ts is not None and current_ts >= existing_ts:
                by_id[job_id] = (job, current_ts)
            elif existing_ts is None and current_ts is None:
                by_id[job_id] = (job, current_ts)

        deduped = [item[0] for item in by_id.values()]
        return deduped, duplicate_count

    def _stage_and_merge(self, jobs, tenant_id):
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
        IF OBJECT_ID('tempdb..#JobsStage') IS NOT NULL
            DROP TABLE #JobsStage;
        """)

        cursor.execute("""
        CREATE TABLE #JobsStage (
            JobId VARCHAR(50) NOT NULL,
            JobNumber VARCHAR(50) NULL,
            Status VARCHAR(50) NULL,
            IssueDescription NVARCHAR(MAX) NULL,
            CustomerId VARCHAR(50) NULL,
            CustomerName NVARCHAR(255) NULL,
            CustomerPropertyName NVARCHAR(255) NULL,
            CustomerRepName NVARCHAR(255) NULL,
            JobTypeName NVARCHAR(255) NULL,
            Priority NVARCHAR(50) NULL,
            CostAmount DECIMAL(18, 2) NULL,
            AmountQuoted DECIMAL(18, 2) NULL,
            DueDate DATETIME NULL,
            CompletedDate DATETIME NULL,
            TenantId NVARCHAR(255) NULL
        );
        """)

        stage_columns = [
            "JobId",
            "JobNumber",
            "Status",
            "IssueDescription",
            "CustomerId",
            "CustomerName",
            "CustomerPropertyName",
            "CustomerRepName",
            "JobTypeName",
            "Priority",
            "CostAmount",
            "AmountQuoted",
            "DueDate",
            "CompletedDate",
            "TenantId",
        ]

        rows = []
        for job in jobs:
            row = (
                safe_str(job.get("id"), 50),
                safe_str(job.get("jobNumber"), 50),
                safe_str(job.get("status"), 50),
                safe_str(job.get("issueDescription")),
                safe_str(job.get("customerId"), 50),
                safe_str(job.get("customerName"), 255),
                safe_str(job.get("customerPropertyName"), 255),
                safe_str(job.get("customerRepName"), 255),
                safe_str(job.get("jobTypeName"), 255),
                safe_str(job.get("priority"), 50),
                safe_decimal(job.get("costAmount")),
                safe_decimal(job.get("amountQuoted")),
                safe_dt(job.get("dueDate")),
                safe_dt(job.get("completedDate")),
                safe_str(tenant_id, 255),
            )

            if len(row) != len(stage_columns):
                raise ValueError(
                    f"Jobs stage row has {len(row)} values, "
                    f"expected {len(stage_columns)}"
                )

            rows.append(row)

        insert_sql = f"""
        INSERT INTO #JobsStage (
            {", ".join(stage_columns)}
        )
        VALUES (
            {", ".join(["?"] * len(stage_columns))}
        );
        """

        cursor.executemany(insert_sql, rows)

        cursor.execute("""
        CREATE UNIQUE CLUSTERED INDEX IX_JobsStage_JobId
        ON #JobsStage (JobId);
        """)

        cursor.execute("""
        MERGE dbo.Jobs AS tgt
        USING #JobsStage AS src
        ON tgt.JobId = src.JobId

        WHEN MATCHED THEN UPDATE SET
            tgt.JobNumber = src.JobNumber,
            tgt.Status = src.Status,
            tgt.IssueDescription = src.IssueDescription,
            tgt.CustomerId = src.CustomerId,
            tgt.CustomerName = src.CustomerName,
            tgt.CustomerPropertyName = src.CustomerPropertyName,
            tgt.CustomerRepName = src.CustomerRepName,
            tgt.JobTypeName = src.JobTypeName,
            tgt.Priority = src.Priority,
            tgt.CostAmount = src.CostAmount,
            tgt.AmountQuoted = src.AmountQuoted,
            tgt.DueDate = src.DueDate,
            tgt.CompletedDate = src.CompletedDate,
            tgt.TenantId = src.TenantId,
            tgt.LastUpdatedDate = GETDATE()

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
        """)

        cursor.execute("SELECT @@ROWCOUNT;")
        merged = cursor.fetchone()[0]

        conn.commit()
        conn.close()

        return int(merged or 0)