"""
Query the project's Sheets with DuckDB from inside a Cirro workspace.

Only workspaces can query Sheets directly: the read permission belongs to the workspace's AWS role.
Everywhere else, use `cirro.sdk.project.DataPortalProject.query_sheets` or `cirro.sdk.sheet.DataPortalSheet`,
which go through the Cirro API and also cover virtual views, column display names and writes.

Sheets are Iceberg tables in the project's Amazon S3 Tables bucket. `connect_sheets` attaches that bucket
to DuckDB as the `sheets` catalog, using the project, account and region Cirro sets on the workspace, so
no Cirro login is needed. Refer to a sheet as `sheets.<namespace>.<table>`.

Requires the `sheets` extra: `pip install "cirro[sheets]"`.

```python
from cirro.helpers.workspace import connect_sheets, query_sheets

counts = query_sheets("SELECT icd_code, COUNT(*) FROM sheets.default.diagnoses GROUP BY icd_code")

conn = connect_sheets()
conn.sql("SELECT * FROM sheets.default.diagnoses").df()
```
"""
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
    from pandas import DataFrame


def connect_sheets() -> 'DuckDBPyConnection':
    """
    Open an in-memory DuckDB connection with the project's Sheets attached as the `sheets` catalog.

    Only works inside a Cirro workspace with Sheets enabled. Credentials come from the workspace's AWS
    role and are read once, when the connection is made; if a long-running session later fails with an
    expired token, call `connect_sheets` again.

    Returns:
        `duckdb.DuckDBPyConnection`
    """
    # Cirro sets this, with the project, account and region, on workspaces whose role can read Sheets
    if os.environ.get("CIRRO_SHEETS_ENABLED") != "true":
        raise RuntimeError("connect_sheets() only works inside a Cirro workspace with Sheets enabled. "
                           "Elsewhere, use project.query_sheets().")
    try:
        import duckdb
    except ImportError:
        raise ImportError("The duckdb library is required to query Sheets directly. "
                          "Please install it using 'pip install \"cirro[sheets]\"'.")

    conn = duckdb.connect()
    conn.execute(_attach_sheets_sql(project_id=os.environ["CIRRO_PROJECT_ID"],
                                    account_id=os.environ["CIRRO_AWS_ACCOUNT_ID"],
                                    region=os.environ["AWS_DEFAULT_REGION"]))
    return conn


def query_sheets(sql: str) -> 'DataFrame':
    """
    Run a query against the project's Sheets and return the result as a Pandas DataFrame.

    Only works inside a Cirro workspace with Sheets enabled. Opens a new connection for each call, so it
    never holds expired credentials. To run many queries, or to keep tables between them, use
    `connect_sheets` instead.

    Args:
        sql (str): SQL to run; refer to a sheet as `sheets.<namespace>.<table>`

    Returns:
        `pandas.DataFrame`
    """
    with connect_sheets() as conn:
        return conn.sql(sql).df()


def _attach_sheets_sql(project_id: str, account_id: str, region: str) -> str:
    # The same statements the workspace image writes to ~/.cirro/duckdb/sheets.sql for the DuckDB CLI
    return f"""
    INSTALL httpfs; INSTALL aws; INSTALL iceberg;
    LOAD httpfs; LOAD aws; LOAD iceberg;
    SET iceberg_unsafe_skip_puffin_verification = true;
    CREATE SECRET IF NOT EXISTS aws (TYPE s3, PROVIDER credential_chain);
    ATTACH 'arn:aws:s3tables:{region}:{account_id}:bucket/project-{project_id}' AS sheets (
        TYPE iceberg,
        ENDPOINT_TYPE s3_tables
    );
    """
