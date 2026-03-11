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


def safe_int(value):
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def safe_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("true", "1", "yes", "y"):
            return True
        if v in ("false", "0", "no", "n"):
            return False
    return None


def safe_str(value, max_len=None):
    if value is None:
        return None
    s = str(value)
    if max_len is not None:
        return s[:max_len]
    return s


class CustomersEntity:
    name = "Customers"

    def sync(self, tenant_id, since=None):
        client = BuildOpsClient(tenant_id=tenant_id)
        customers = client.fetch_all_customers(updated_after=since)

        rows_fetched = len(customers)
        if rows_fetched == 0:
            print("[OBSERVE] Customers: no rows fetched")
            return 0, 0

        max_seen_updated_at = None
        for cust in customers:
            ts = client.extract_updated_timestamp(cust)
            if ts and (not max_seen_updated_at or ts > max_seen_updated_at):
                max_seen_updated_at = ts

        print(
            f"[OBSERVE] Customers max updated timestamp: "
            f"{max_seen_updated_at}"
        )

        deduped_customers, duplicate_count = self._dedupe_customers(
            customers,
            client
        )

        if duplicate_count > 0:
            print(
                f"[OBSERVE] Customers duplicates removed: {duplicate_count} "
                f"(kept {len(deduped_customers)} unique ids out of {len(customers)})"
            )

        rows_merged = self._stage_and_merge(deduped_customers, tenant_id)
        return rows_fetched, rows_merged

    def _dedupe_customers(self, customers, client):
        """
        Deduplicate customers by id, keeping the record with the latest
        observed updated timestamp. If timestamps are equal or missing,
        keep the later occurrence.
        """
        by_id = {}
        duplicate_count = 0

        for cust in customers:
            cust_id = safe_str(cust.get("id"), 36)

            # Skip rows with no primary key
            if not cust_id:
                continue

            current_ts = client.extract_updated_timestamp(cust)

            if cust_id not in by_id:
                by_id[cust_id] = (cust, current_ts)
                continue

            duplicate_count += 1
            existing_cust, existing_ts = by_id[cust_id]

            if existing_ts is None and current_ts is not None:
                by_id[cust_id] = (cust, current_ts)
            elif existing_ts is not None and current_ts is not None and current_ts >= existing_ts:
                by_id[cust_id] = (cust, current_ts)
            elif existing_ts is None and current_ts is None:
                by_id[cust_id] = (cust, current_ts)

        deduped = [item[0] for item in by_id.values()]
        return deduped, duplicate_count

    def _stage_and_merge(self, customers, tenant_id):
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
        IF OBJECT_ID('tempdb..#CustomersStage') IS NOT NULL
            DROP TABLE #CustomersStage;
        """)

        cursor.execute("""
        CREATE TABLE #CustomersStage (
            id NVARCHAR(36) NOT NULL,
            name NVARCHAR(255) NULL,
            accountNumber NVARCHAR(100) NULL,
            customerType NVARCHAR(100) NULL,
            isActive BIT NULL,
            email NVARCHAR(255) NULL,
            customerNumber NVARCHAR(50) NULL,
            creditLimit DECIMAL(18, 2) NULL,
            isTaxable BIT NULL,
            taxRateValue DECIMAL(18, 4) NULL,
            taxCodeId NVARCHAR(36) NULL,
            sameAddress BIT NULL,
            phonePrimary NVARCHAR(50) NULL,
            phoneAlternate NVARCHAR(50) NULL,
            receiveSMS BIT NULL,
            taxExemptIdValue NVARCHAR(100) NULL,
            taxRateId NVARCHAR(36) NULL,
            paymentTermId NVARCHAR(36) NULL,
            invoicePresetId NVARCHAR(36) NULL,
            invoiceDeliveryPref NVARCHAR(50) NULL,
            priceBookId NVARCHAR(36) NULL,
            status NVARCHAR(50) NULL,
            accountingRefId NVARCHAR(100) NULL,
            accountingVersion NVARCHAR(50) NULL,
            accountingSyncStatus NVARCHAR(50) NULL,
            prospectiveStatus NVARCHAR(50) NULL,
            addresses_totalCount INT NULL,
            address0_id NVARCHAR(36) NULL,
            address0_billTo NVARCHAR(255) NULL,
            address0_shipTo NVARCHAR(255) NULL,
            address0_addressType NVARCHAR(50) NULL,
            address0_addressLine1 NVARCHAR(255) NULL,
            address0_addressLine2 NVARCHAR(255) NULL,
            address0_city NVARCHAR(100) NULL,
            address0_country NVARCHAR(50) NULL,
            address0_state NVARCHAR(50) NULL,
            address0_zipcode NVARCHAR(20) NULL,
            address0_latitude DECIMAL(18, 15) NULL,
            address0_longitude DECIMAL(18, 15) NULL,
            address0_status NVARCHAR(50) NULL,
            address0_customerId NVARCHAR(36) NULL,
            address0_version INT NULL,
            address0_isActive BIT NULL,
            address0_audit_createdBy NVARCHAR(255) NULL,
            address0_audit_createdDate DATETIME2(3) NULL,
            address0_audit_createdDateTime BIGINT NULL,
            address0_audit_deletedBy NVARCHAR(255) NULL,
            address0_audit_deletedDate NVARCHAR(50) NULL,
            address0_audit_deletedDateTime NVARCHAR(50) NULL,
            address0_audit_lastUpdatedBy NVARCHAR(255) NULL,
            address0_audit_lastUpdatedDate DATETIME2(3) NULL,
            address0_audit_lastUpdatedDateTime BIGINT NULL,
            audit_createdBy NVARCHAR(255) NULL,
            audit_createdDate DATETIME2(3) NULL,
            audit_createdDateTime BIGINT NULL,
            audit_deletedBy NVARCHAR(255) NULL,
            audit_deletedDate NVARCHAR(50) NULL,
            audit_deletedDateTime NVARCHAR(50) NULL,
            audit_lastUpdatedBy NVARCHAR(255) NULL,
            audit_lastUpdatedDate DATETIME2(3) NULL,
            audit_lastUpdatedDateTime BIGINT NULL,
            logoUrl NVARCHAR(500) NULL,
            websiteUrl NVARCHAR(500) NULL,
            version INT NULL,
            tenantId NVARCHAR(36) NULL,
            tenantCompanyId NVARCHAR(36) NULL,
            amountNotToExceed DECIMAL(18, 2) NULL
        );
        """)

        stage_columns = [
            "id", "name", "accountNumber", "customerType", "isActive", "email",
            "customerNumber", "creditLimit", "isTaxable", "taxRateValue",
            "taxCodeId", "sameAddress", "phonePrimary", "phoneAlternate",
            "receiveSMS", "taxExemptIdValue", "taxRateId", "paymentTermId",
            "invoicePresetId", "invoiceDeliveryPref", "priceBookId", "status",
            "accountingRefId", "accountingVersion", "accountingSyncStatus",
            "prospectiveStatus", "addresses_totalCount", "address0_id",
            "address0_billTo", "address0_shipTo", "address0_addressType",
            "address0_addressLine1", "address0_addressLine2", "address0_city",
            "address0_country", "address0_state", "address0_zipcode",
            "address0_latitude", "address0_longitude", "address0_status",
            "address0_customerId", "address0_version", "address0_isActive",
            "address0_audit_createdBy", "address0_audit_createdDate",
            "address0_audit_createdDateTime", "address0_audit_deletedBy",
            "address0_audit_deletedDate", "address0_audit_deletedDateTime",
            "address0_audit_lastUpdatedBy", "address0_audit_lastUpdatedDate",
            "address0_audit_lastUpdatedDateTime", "audit_createdBy",
            "audit_createdDate", "audit_createdDateTime", "audit_deletedBy",
            "audit_deletedDate", "audit_deletedDateTime", "audit_lastUpdatedBy",
            "audit_lastUpdatedDate", "audit_lastUpdatedDateTime", "logoUrl",
            "websiteUrl", "version", "tenantId", "tenantCompanyId",
            "amountNotToExceed"
        ]

        rows = []
        for cust in customers:
            addresses = cust.get("addresses", {}) or {}
            addr_items = addresses.get("items", []) or []
            addr0 = addr_items[0] if addr_items else {}

            acct = cust.get("accountingAttributes", {}) or {}
            audit = cust.get("audit", {}) or {}
            addr_audit = addr0.get("audit", {}) or {}

            row = (
                safe_str(cust.get("id"), 36),
                safe_str(cust.get("name"), 255),
                safe_str(cust.get("accountNumber"), 100),
                safe_str(cust.get("customerType"), 100),
                safe_bool(cust.get("isActive")),
                safe_str(cust.get("email"), 255),
                safe_str(cust.get("customerNumber"), 50),
                safe_decimal(cust.get("creditLimit")),
                safe_bool(cust.get("isTaxable")),
                safe_decimal(cust.get("taxRateValue")),
                safe_str(cust.get("taxCodeId"), 36),
                safe_bool(cust.get("sameAddress")),
                safe_str(cust.get("phonePrimary"), 50),
                safe_str(cust.get("phoneAlternate"), 50),
                safe_bool(cust.get("receiveSMS")),
                safe_str(cust.get("taxExemptIdValue"), 100),
                safe_str(cust.get("taxRateId"), 36),
                safe_str(cust.get("paymentTermId"), 36),
                safe_str(cust.get("invoicePresetId"), 36),
                safe_str(cust.get("invoiceDeliveryPref"), 50),
                safe_str(cust.get("priceBookId"), 36),
                safe_str(cust.get("status"), 50),
                safe_str(acct.get("accountingRefId"), 100),
                safe_str(acct.get("accountingVersion"), 50),
                safe_str(acct.get("syncStatus"), 50),
                safe_str(cust.get("prospectiveStatus"), 50),
                safe_int(addresses.get("totalCount")),

                safe_str(addr0.get("id"), 36),
                safe_str(addr0.get("billTo"), 255),
                safe_str(addr0.get("shipTo"), 255),
                safe_str(addr0.get("addressType"), 50),
                safe_str(addr0.get("addressLine1"), 255),
                safe_str(addr0.get("addressLine2"), 255),
                safe_str(addr0.get("city"), 100),
                safe_str(addr0.get("country"), 50),
                safe_str(addr0.get("state"), 50),
                safe_str(addr0.get("zipcode"), 20),
                safe_decimal(addr0.get("latitude")),
                safe_decimal(addr0.get("longitude")),
                safe_str(addr0.get("status"), 50),
                safe_str(addr0.get("customerId"), 36),
                safe_int(addr0.get("version")),
                safe_bool(addr0.get("isActive")),

                safe_str((addr_audit.get("createdBy", {}) or {}).get("username"), 255),
                safe_dt(addr_audit.get("createdDate")),
                safe_int(addr_audit.get("createdDateTime")),
                safe_str((addr_audit.get("deletedBy", {}) or {}).get("username"), 255),
                safe_str(addr_audit.get("deletedDate"), 50),
                safe_str(addr_audit.get("deletedDateTime"), 50),
                safe_str((addr_audit.get("lastUpdatedBy", {}) or {}).get("username"), 255),
                safe_dt(addr_audit.get("lastUpdatedDate")),
                safe_int(addr_audit.get("lastUpdatedDateTime")),

                safe_str((audit.get("createdBy", {}) or {}).get("username"), 255),
                safe_dt(audit.get("createdDate")),
                safe_int(audit.get("createdDateTime")),
                safe_str((audit.get("deletedBy", {}) or {}).get("username"), 255),
                safe_str(audit.get("deletedDate"), 50),
                safe_str(audit.get("deletedDateTime"), 50),
                safe_str((audit.get("lastUpdatedBy", {}) or {}).get("username"), 255),
                safe_dt(audit.get("lastUpdatedDate")),
                safe_int(audit.get("lastUpdatedDateTime")),

                safe_str(cust.get("logoUrl"), 500),
                safe_str(cust.get("websiteUrl"), 500),
                safe_int(cust.get("version")),
                safe_str(tenant_id, 36),
                safe_str(cust.get("tenantCompanyId"), 36),
                safe_decimal(cust.get("amountNotToExceed")),
            )

            if len(row) != len(stage_columns):
                raise ValueError(
                    f"Customers stage row has {len(row)} values, "
                    f"expected {len(stage_columns)}"
                )

            rows.append(row)

        insert_sql = f"""
        INSERT INTO #CustomersStage (
            {", ".join(stage_columns)}
        )
        VALUES (
            {", ".join(["?"] * len(stage_columns))}
        );
        """

        cursor.executemany(insert_sql, rows)

        cursor.execute("""
        CREATE UNIQUE CLUSTERED INDEX IX_CustomersStage_id
        ON #CustomersStage (id);
        """)

        cursor.execute("""
        MERGE dbo.Customers AS tgt
        USING #CustomersStage AS src
        ON tgt.id = src.id

        WHEN MATCHED THEN UPDATE SET
            tgt.name = src.name,
            tgt.accountNumber = src.accountNumber,
            tgt.customerType = src.customerType,
            tgt.isActive = src.isActive,
            tgt.email = src.email,
            tgt.customerNumber = src.customerNumber,
            tgt.creditLimit = src.creditLimit,
            tgt.isTaxable = src.isTaxable,
            tgt.taxRateValue = src.taxRateValue,
            tgt.taxCodeId = src.taxCodeId,
            tgt.sameAddress = src.sameAddress,
            tgt.phonePrimary = src.phonePrimary,
            tgt.phoneAlternate = src.phoneAlternate,
            tgt.receiveSMS = src.receiveSMS,
            tgt.taxExemptIdValue = src.taxExemptIdValue,
            tgt.taxRateId = src.taxRateId,
            tgt.paymentTermId = src.paymentTermId,
            tgt.invoicePresetId = src.invoicePresetId,
            tgt.invoiceDeliveryPref = src.invoiceDeliveryPref,
            tgt.priceBookId = src.priceBookId,
            tgt.status = src.status,
            tgt.accountingRefId = src.accountingRefId,
            tgt.accountingVersion = src.accountingVersion,
            tgt.accountingSyncStatus = src.accountingSyncStatus,
            tgt.prospectiveStatus = src.prospectiveStatus,
            tgt.addresses_totalCount = src.addresses_totalCount,
            tgt.address0_id = src.address0_id,
            tgt.address0_billTo = src.address0_billTo,
            tgt.address0_shipTo = src.address0_shipTo,
            tgt.address0_addressType = src.address0_addressType,
            tgt.address0_addressLine1 = src.address0_addressLine1,
            tgt.address0_addressLine2 = src.address0_addressLine2,
            tgt.address0_city = src.address0_city,
            tgt.address0_country = src.address0_country,
            tgt.address0_state = src.address0_state,
            tgt.address0_zipcode = src.address0_zipcode,
            tgt.address0_latitude = src.address0_latitude,
            tgt.address0_longitude = src.address0_longitude,
            tgt.address0_status = src.address0_status,
            tgt.address0_customerId = src.address0_customerId,
            tgt.address0_version = src.address0_version,
            tgt.address0_isActive = src.address0_isActive,
            tgt.address0_audit_createdBy = src.address0_audit_createdBy,
            tgt.address0_audit_createdDate = src.address0_audit_createdDate,
            tgt.address0_audit_createdDateTime = src.address0_audit_createdDateTime,
            tgt.address0_audit_deletedBy = src.address0_audit_deletedBy,
            tgt.address0_audit_deletedDate = src.address0_audit_deletedDate,
            tgt.address0_audit_deletedDateTime = src.address0_audit_deletedDateTime,
            tgt.address0_audit_lastUpdatedBy = src.address0_audit_lastUpdatedBy,
            tgt.address0_audit_lastUpdatedDate = src.address0_audit_lastUpdatedDate,
            tgt.address0_audit_lastUpdatedDateTime = src.address0_audit_lastUpdatedDateTime,
            tgt.audit_createdBy = src.audit_createdBy,
            tgt.audit_createdDate = src.audit_createdDate,
            tgt.audit_createdDateTime = src.audit_createdDateTime,
            tgt.audit_deletedBy = src.audit_deletedBy,
            tgt.audit_deletedDate = src.audit_deletedDate,
            tgt.audit_deletedDateTime = src.audit_deletedDateTime,
            tgt.audit_lastUpdatedBy = src.audit_lastUpdatedBy,
            tgt.audit_lastUpdatedDate = src.audit_lastUpdatedDate,
            tgt.audit_lastUpdatedDateTime = src.audit_lastUpdatedDateTime,
            tgt.logoUrl = src.logoUrl,
            tgt.websiteUrl = src.websiteUrl,
            tgt.version = src.version,
            tgt.tenantId = src.tenantId,
            tgt.tenantCompanyId = src.tenantCompanyId,
            tgt.amountNotToExceed = src.amountNotToExceed

        WHEN NOT MATCHED THEN INSERT (
            id, name, accountNumber, customerType, isActive, email,
            customerNumber, creditLimit, isTaxable, taxRateValue, taxCodeId,
            sameAddress, phonePrimary, phoneAlternate, receiveSMS,
            taxExemptIdValue, taxRateId, paymentTermId, invoicePresetId,
            invoiceDeliveryPref, priceBookId, status, accountingRefId,
            accountingVersion, accountingSyncStatus, prospectiveStatus,
            addresses_totalCount, address0_id, address0_billTo, address0_shipTo,
            address0_addressType, address0_addressLine1, address0_addressLine2,
            address0_city, address0_country, address0_state, address0_zipcode,
            address0_latitude, address0_longitude, address0_status,
            address0_customerId, address0_version, address0_isActive,
            address0_audit_createdBy, address0_audit_createdDate,
            address0_audit_createdDateTime, address0_audit_deletedBy,
            address0_audit_deletedDate, address0_audit_deletedDateTime,
            address0_audit_lastUpdatedBy, address0_audit_lastUpdatedDate,
            address0_audit_lastUpdatedDateTime, audit_createdBy,
            audit_createdDate, audit_createdDateTime, audit_deletedBy,
            audit_deletedDate, audit_deletedDateTime, audit_lastUpdatedBy,
            audit_lastUpdatedDate, audit_lastUpdatedDateTime, logoUrl,
            websiteUrl, version, tenantId, tenantCompanyId, amountNotToExceed
        )
        VALUES (
            src.id, src.name, src.accountNumber, src.customerType, src.isActive,
            src.email, src.customerNumber, src.creditLimit, src.isTaxable,
            src.taxRateValue, src.taxCodeId, src.sameAddress, src.phonePrimary,
            src.phoneAlternate, src.receiveSMS, src.taxExemptIdValue,
            src.taxRateId, src.paymentTermId, src.invoicePresetId,
            src.invoiceDeliveryPref, src.priceBookId, src.status,
            src.accountingRefId, src.accountingVersion,
            src.accountingSyncStatus, src.prospectiveStatus,
            src.addresses_totalCount, src.address0_id, src.address0_billTo,
            src.address0_shipTo, src.address0_addressType,
            src.address0_addressLine1, src.address0_addressLine2, src.address0_city,
            src.address0_country, src.address0_state, src.address0_zipcode,
            src.address0_latitude, src.address0_longitude, src.address0_status,
            src.address0_customerId, src.address0_version, src.address0_isActive,
            src.address0_audit_createdBy, src.address0_audit_createdDate,
            src.address0_audit_createdDateTime, src.address0_audit_deletedBy,
            src.address0_audit_deletedDate, src.address0_audit_deletedDateTime,
            src.address0_audit_lastUpdatedBy, src.address0_audit_lastUpdatedDate,
            src.address0_audit_lastUpdatedDateTime, src.audit_createdBy,
            src.audit_createdDate, src.audit_createdDateTime, src.audit_deletedBy,
            src.audit_deletedDate, src.audit_deletedDateTime,
            src.audit_lastUpdatedBy, src.audit_lastUpdatedDate,
            src.audit_lastUpdatedDateTime, src.logoUrl, src.websiteUrl,
            src.version, src.tenantId, src.tenantCompanyId,
            src.amountNotToExceed
        );
        """)

        cursor.execute("SELECT @@ROWCOUNT;")
        merged = cursor.fetchone()[0]

        conn.commit()
        conn.close()

        return int(merged or 0)