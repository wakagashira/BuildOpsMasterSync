from buildops_master_sync.connectors.buildops_client import BuildOpsClient
from buildops_master_sync.connectors.sql import get_connection


class EmployeesEntity:
    name = "Employees"

    def sync(self, tenant_id, since=None):

        client = BuildOpsClient(tenant_id=tenant_id)
        employees = client.fetch_all_employees()

        conn = get_connection()
        cursor = conn.cursor()

        # recreate temp stage
        cursor.execute("""
        IF OBJECT_ID('tempdb..#EmployeesStage') IS NOT NULL
            DROP TABLE #EmployeesStage
        """)

        cursor.execute("""
        CREATE TABLE #EmployeesStage(
            id NVARCHAR(36),
            name NVARCHAR(255),
            email NVARCHAR(255),
            firstName NVARCHAR(255),
            lastName NVARCHAR(255),
            isActive BIT,
            tenantId NVARCHAR(36)
        )
        """)

        rows = []

        for emp in employees:

            rows.append((
                emp.get("id"),
                emp.get("name"),
                emp.get("email"),
                emp.get("firstName"),
                emp.get("lastName"),
                emp.get("isActive"),
                tenant_id
            ))

        cursor.fast_executemany = True

        cursor.executemany("""
        INSERT INTO #EmployeesStage
        (
            id,
            name,
            email,
            firstName,
            lastName,
            isActive,
            tenantId
        )
        VALUES (?,?,?,?,?,?,?)
        """, rows)

        cursor.execute("""
        MERGE dbo.Employees AS tgt
        USING #EmployeesStage AS src
        ON tgt.id = src.id

        WHEN MATCHED THEN
            UPDATE SET
                tgt.name = src.name,
                tgt.email = src.email,
                tgt.firstName = src.firstName,
                tgt.lastName = src.lastName,
                tgt.isActive = src.isActive,
                tgt.tenantId = src.tenantId

        WHEN NOT MATCHED THEN
            INSERT
            (
                id,
                name,
                email,
                firstName,
                lastName,
                isActive,
                tenantId
            )
            VALUES
            (
                src.id,
                src.name,
                src.email,
                src.firstName,
                src.lastName,
                src.isActive,
                src.tenantId
            );
        """)

        cursor.execute("SELECT @@ROWCOUNT")
        merged = cursor.fetchone()[0]

        conn.commit()
        conn.close()

        return len(employees), merged