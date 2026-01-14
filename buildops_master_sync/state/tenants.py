"""
Tenant state management for Build Ops Master Sync.

Responsibilities:
- Load active, enabled tenants
- Update tenant success state
- Update tenant failure state
- Maintain incremental sync watermark
"""

from datetime import datetime


# ----------------------------------------------------------------------
# Tenant discovery
# ----------------------------------------------------------------------

def load_active_tenants(cursor):
    """
    Load tenants eligible for sync.

    Criteria:
    - Active = 1
    - sync_enabled = 1
    """

    cursor.execute("""
        SELECT
            Tenant,
            Name,
            Type,
            Active,
            last_synced_at
        FROM dbo.Tenants
        WHERE Active = 1
          AND sync_enabled = 1
    """)

    columns = [col[0] for col in cursor.description]
    tenants = []

    for row in cursor.fetchall():
        tenants.append(dict(zip(columns, row)))

    return tenants


# ----------------------------------------------------------------------
# Tenant success handling
# ----------------------------------------------------------------------

def update_tenant_success(cursor, tenant_id):
    """
    Update tenant after a successful full entity run.
    """

    cursor.execute("""
        UPDATE dbo.Tenants
        SET
            last_synced_at = ?,
            last_successful_run_at = ?,
            last_error = NULL
        WHERE Tenant = ?
    """, (
        datetime.utcnow(),
        datetime.utcnow(),
        tenant_id,
    ))


# ----------------------------------------------------------------------
# Tenant failure handling
# ----------------------------------------------------------------------

def update_tenant_failure(cursor, tenant_id, error_message):
    """
    Update tenant after a failed run.
    """

    cursor.execute("""
        UPDATE dbo.Tenants
        SET
            last_failed_run_at = ?,
            last_error = ?
        WHERE Tenant = ?
    """, (
        datetime.utcnow(),
        error_message[:2000],  # defensive truncate
        tenant_id,
    ))
