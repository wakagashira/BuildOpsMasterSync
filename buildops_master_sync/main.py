import os
from dotenv import load_dotenv

from buildops_master_sync.core.engine import MasterSyncEngine
from buildops_master_sync.core.policies import SyncPolicy
from buildops_master_sync.state.tenants import TenantRepository
from buildops_master_sync.state.runs import RunRepository
from buildops_master_sync.entities.jobs import JobsEntity
from buildops_master_sync.entities.customers import CustomersEntity
from buildops_master_sync.entities.quotes import QuotesEntity


def _parse_entities(env_value: str):
    parts = [p.strip() for p in (env_value or "").split(",") if p.strip()]
    if not parts:
        return ["Jobs", "Customers", "Quotes"]
    return parts


def build_engine() -> MasterSyncEngine:
    load_dotenv()

    sql_conn_str = os.getenv("SQL_CONNECTION_STRING")
    if not sql_conn_str:
        raise ValueError("SQL_CONNECTION_STRING is not set")

    client_id = os.getenv("BUILDOPS_CLIENT_ID")
    client_secret = os.getenv("BUILDOPS_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise ValueError("BUILDOPS_CLIENT_ID and BUILDOPS_CLIENT_SECRET must be set")

    policy = SyncPolicy.from_env()

    tenant_repo = TenantRepository(sql_conn_str)
    run_repo = RunRepository(sql_conn_str)

    entity_registry = {
        "Jobs": JobsEntity(sql_conn_str),
        "Customers": CustomersEntity(sql_conn_str),
        "Quotes": QuotesEntity(sql_conn_str),
    }

    entities_to_run = _parse_entities(os.getenv("ENTITIES", "Jobs,Customers,Quotes"))
    entities = [entity_registry[name] for name in entities_to_run if name in entity_registry]

    if not entities:
        raise ValueError("No valid ENTITIES configured. Supported: Jobs,Customers,Quotes")

    return MasterSyncEngine(
        sql_connection_string=sql_conn_str,
        buildops_client_id=client_id,
        buildops_client_secret=client_secret,
        tenant_repo=tenant_repo,
        run_repo=run_repo,
        policy=policy,
        entities=entities,
    )


def main():
    engine = build_engine()
    result = engine.run()
    # Non-zero exit on failure
    if result.get("status") in ("FAILED", "PARTIAL"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
