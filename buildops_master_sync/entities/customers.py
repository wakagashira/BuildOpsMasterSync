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


class CustomersEntity:
    name = "Customers"

    def sync(self, tenant_id, since=None):
        client = BuildOpsClient(tenant_id=tenant_id)
        customers = client.fetch_all_customers(updated_after=since)

        rows_fetched = len(customers)
        if rows_fetched == 0:
            print("[OBSERVE] Customers: no rows fetched")
            return 0, 0

        # ------------------------------------------------------------
        # Phase 1: Incremental correctness observation
        # ------------------------------------------------------------
        max_seen_updated_at = None
        for cust in customers:
            ts = client.extract_updated_timestamp(cust)
            if ts and (not max_seen_updated_at or ts > max_seen_updated_at):
                max_seen_updated_at = ts

        print(
            f"[OBSERVE] Customers max updated timestamp: "
            f"{max_seen_updated_at}"
        )

        rows_merged = self._upsert(customers, tenant_id)
        return rows_fetched, rows_merged

    def _upsert(self, customers, tenant_id):
        conn = get_connection()
        cursor = conn.cursor()

        upsert_sql = """
        MERGE dbo.Customers AS tgt
        USING (VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,          -- 23
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,          -- 22 (45)
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?           -- 22 (67)
        )) AS src (
            id, name, accountNumber, customerType, isActive, email, customerNumber,
            creditLimit, isTaxable, taxRateValue, taxCodeId, sameAddress,
            phonePrimary, phoneAlternate, receiveSMS, taxExemptIdValue,
            taxRateId, paymentTermId, invoicePresetId, invoiceDeliveryPref,
            priceBookId, status, accountingRefId, accountingVersion,

            accountingSyncStatus, prospectiveStatus, addresses_totalCount,
            address0_id, address0_billTo, address0_shipTo, address0_addressType,
            address0_addressLine1, address0_addressLine2, address0_city,
            address0_country, address0_state, address0_zipcode,
            address0_latitude, address0_longitude, address0_status,
            address0_customerId, address0_version, address0_isActive,

            address0_audit_createdBy, address0_audit_createdDate,
            address0_audit_createdDateTime, address0_audit_deletedBy,
            address0_audit_deletedDate, address0_audit_deletedDateTime,
            address0_audit_lastUpdatedBy, address0_audit_lastUpdatedDate,
            address0_audit_lastUpdatedDateTime,

            audit_createdBy, audit_createdDate, audit_createdDateTime,
            audit_deletedBy, audit_deletedDate, audit_deletedDateTime,
            audit_lastUpdatedBy, audit_lastUpdatedDate,
            audit_lastUpdatedDateTime,

            logoUrl, websiteUrl, version, tenantId, tenantCompanyId,
            amountNotToExceed
        )
        ON tgt.id = src.id
        WHEN MATCHED THEN UPDATE SET
            name = src.name
        WHEN NOT MATCHED THEN INSERT (
            id, name, tenantId
        )
        VALUES (
            src.id, src.name, src.tenantId
        );
        """

        merged = 0

        for cust in customers:
            cursor.execute(
                upsert_sql,
                (
                    cust.get("id"),
                    cust.get("name"),
                    cust.get("accountNumber"),
                    cust.get("customerType"),
                    cust.get("isActive"),
                    cust.get("email"),
                    cust.get("customerNumber"),
                    cust.get("creditLimit"),
                    cust.get("isTaxable"),
                    cust.get("taxRateValue"),
                    cust.get("taxCodeId"),
                    cust.get("sameAddress"),
                    cust.get("phonePrimary"),
                    cust.get("phoneAlternate"),
                    cust.get("receiveSMS"),
                    cust.get("taxExemptIdValue"),
                    cust.get("taxRateId"),
                    cust.get("paymentTermId"),
                    cust.get("invoicePresetId"),
                    cust.get("invoiceDeliveryPref"),
                    cust.get("priceBookId"),
                    cust.get("status"),
                    cust.get("accountingAttributes", {}).get("accountingRefId"),
                    cust.get("accountingAttributes", {}).get("accountingVersion"),

                    cust.get("accountingAttributes", {}).get("syncStatus"),
                    cust.get("prospectiveStatus"),
                    cust.get("addresses", {}).get("totalCount"),

                    None, None, None, None, None, None, None, None,
                    None, None, None, None, None, None, None, None,
                    None, None, None, None, None, None, None, None,
                    None, None, None, None, None, None, None, None,
                    None, None, None,

                    cust.get("logoUrl"),
                    cust.get("websiteUrl"),
                    cust.get("version"),
                    tenant_id,
                    cust.get("tenantCompanyId"),
                    cust.get("amountNotToExceed"),
                ),
            )
            merged += 1

        conn.commit()
        conn.close()
        return merged
