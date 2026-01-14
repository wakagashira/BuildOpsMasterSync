
import os
import pyodbc


def get_connection():
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE", "BuildOps")
    user = os.getenv("SQL_USER")
    password = os.getenv("SQL_PASSWORD")

    if not all([server, database, user, password]):
        raise ValueError(
            "SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD must all be set"
        )

    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password}"
    )

    return pyodbc.connect(conn_str)
