import os
import unittest
from unittest.mock import patch

import duckdb

from cirro.helpers.workspace import connect_sheets, query_sheets

_WORKSPACE_ENV = {
    "CIRRO_SHEETS_ENABLED": "true",
    "CIRRO_PROJECT_ID": "project-1",
    "CIRRO_AWS_ACCOUNT_ID": "123456789012",
    "AWS_DEFAULT_REGION": "us-west-2"
}


class TestConnectSheets(unittest.TestCase):
    def _attach_sql(self, env):
        with patch.dict(os.environ, env, clear=True), patch("duckdb.connect") as duckdb_connect:
            connect_sheets()
        return duckdb_connect.return_value.execute.call_args.args[0]

    def test_attaches_project_bucket(self):
        sql = self._attach_sql(_WORKSPACE_ENV)
        self.assertIn("ATTACH 'arn:aws:s3tables:us-west-2:123456789012:bucket/project-project-1' AS sheets", sql)

    def test_refuses_outside_workspace(self):
        with self.assertRaisesRegex(RuntimeError, "only works inside a Cirro workspace"):
            self._attach_sql({})

    def test_refuses_when_sheets_disabled(self):
        with self.assertRaisesRegex(RuntimeError, "only works inside a Cirro workspace"):
            self._attach_sql({**_WORKSPACE_ENV, "CIRRO_SHEETS_ENABLED": "false"})


class TestQuerySheets(unittest.TestCase):
    def test_returns_dataframe_and_closes_connection(self):
        conn = duckdb.connect()
        with patch("cirro.helpers.workspace.connect_sheets", return_value=conn):
            df = query_sheets("SELECT 42 AS answer")

        self.assertEqual(df["answer"].tolist(), [42])
        with self.assertRaises(duckdb.ConnectionException):
            conn.sql("SELECT 1")
