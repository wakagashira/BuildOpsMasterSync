"""
Run state management for Build Ops Master Sync.

Responsibilities:
- Create and finalize sync runs
- Track per-entity execution per tenant
- Record row counts and failures
"""

from datetime import datetime
import uuid


# ----------------------------------------------------------------------
# Sync run lifecycle
# ----------------------------------------------------------------------

def create_sync_run(cursor, sync_mode, initiated_by):
    """
    Create a new sync run.

    Returns:
        run_id (UUID)
    """
    run_id = uuid.uuid4()

    cursor.execute("""
        INSERT INTO dbo.SyncRuns (
            run_id,
            started_at,
            status,
            sync_mode,
            initiated_by
        )
        VALUES (?, ?, ?, ?, ?)
    """, (
        run_id,
        datetime.utcnow(),
        "RUNNING",
        sync_mode.upper(),
        initiated_by,
    ))

    return run_id


def finish_sync_run(cursor, run_id, status, error_summary=None):
    """
    Finalize a sync run.
    """
    cursor.execute("""
        UPDATE dbo.SyncRuns
        SET
            finished_at = ?,
            status = ?,
            error_summary = ?
        WHERE run_id = ?
    """, (
        datetime.utcnow(),
        status,
        error_summary,
        run_id,
    ))


# ----------------------------------------------------------------------
# Entity run lifecycle
# ----------------------------------------------------------------------

def start_entity_run(cursor, run_id, tenant_id, entity_name):
    """
    Start tracking an entity run for a tenant.

    Returns:
        entity_run_id (UUID)
    """
    entity_run_id = uuid.uuid4()

    cursor.execute("""
        INSERT INTO dbo.SyncEntityRuns (
            entity_run_id,
            run_id,
            tenant,
            entity_name,
            started_at,
            status
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        entity_run_id,
        run_id,
        tenant_id,
        entity_name,
        datetime.utcnow(),
        "RUNNING",
    ))

    return entity_run_id


def finish_entity_run(
    cursor,
    entity_run_id,
    status,
    rows_fetched=None,
    rows_merged=None,
    error_message=None,
):
    """
    Finalize an entity run.
    """
    cursor.execute("""
        UPDATE dbo.SyncEntityRuns
        SET
            finished_at = ?,
            status = ?,
            rows_fetched = ?,
            rows_merged = ?,
            error_message = ?
        WHERE entity_run_id = ?
    """, (
        datetime.utcnow(),
        status,
        rows_fetched,
        rows_merged,
        error_message[:2000] if error_message else None,
        entity_run_id,
    ))
