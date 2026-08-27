"""
Python SDK and command-line interface for the [Cirro](https://cirro.bio) platform.

Install with `pip install cirro`.

## Authentication

`DataPortal` needs to know which Cirro instance to talk to and how to
authenticate. The instance comes from the `base_url` argument, falling back to
the `CIRRO_BASE_URL` environment variable, then to the saved configuration in
`~/.cirro/config.ini` (written by `cirro configure`).

There are three ways to authenticate:

1. **Interactive (the default).** `DataPortal()` uses the saved configuration.
   If none exists it starts a device-code login, prints a URL, and **blocks
   until the user completes the login in a browser**. Avoid this in scripts and
   automated sessions -- there is nobody to click the link, so it will hang
   until the device code expires.

2. **Headless.** OAuth client credentials never prompt, so this is the option
   to use for automation:

   ```python
   import os
   from cirro import CirroApi, DataPortal
   from cirro.auth.client_creds import ClientCredentialsAuth
   from cirro.config import AppConfig

   config = AppConfig(base_url="app.cirro.bio")
   auth = ClientCredentialsAuth(
       os.environ["CIRRO_CLIENT_ID"],
       os.environ["CIRRO_CLIENT_SECRET"],
       auth_endpoint=config.auth_endpoint
   )
   portal = DataPortal(client=CirroApi(auth_info=auth))
   ```

   See [OAuth Apps](https://docs.cirro.bio/cli-sdk/oauth-apps/) for how to
   create the client ID and secret.

3. **Non-blocking browser login.** `cirro.sdk.login.DataPortalLogin` returns
   the authorization message so you can display it yourself, and blocks only
   when you call `await_completion()`.

## Quickstart

```python
from cirro import DataPortal

portal = DataPortal(base_url="app.cirro.bio")

# Browse
for project in portal.list_projects():
    print(project.name)

# Read a file straight into a DataFrame, without downloading it
df = portal.read_file("Name of Project", "Name of Dataset", glob="*.csv")

# Launch an analysis on an existing dataset
dataset = portal.get_dataset(project="Name of Project", dataset="Name of Dataset")
new_dataset_id = dataset.run_analysis(
    name="Name of the output dataset",
    process="Name or ID of the process to run",
    params={}
)
```

## Object model

- `cirro.sdk.portal.DataPortal` -- entry point; lists projects, processes,
  and reference types.
- `cirro.sdk.project.DataPortalProject` -- a permissions boundary holding
  datasets and reference data; uploads new datasets.
- `cirro.sdk.dataset.DataPortalDataset` -- a collection of files, either
  uploaded or produced by an analysis; reads files and launches analyses.
- `cirro.sdk.file.DataPortalFile` -- one file; read it into memory
  (`read_csv`, `read_json`, ...) or download it.
- `cirro.sdk.process.DataPortalProcess` -- a pipeline that can be run, or a
  data type that datasets can be uploaded as.
- `cirro.sdk.task.DataPortalTask` -- one task from a Nextflow execution, used
  for debugging failed analyses.
- `cirro.sdk.reference.DataPortalReference` -- reference data (genomes,
  annotations) available to a project.
- `cirro.cirro_client.CirroApi` -- the lower-level typed API client; use it
  when the classes above do not cover what you need.

Projects, datasets, and processes can be looked up by either name or ID --
`get_project`, `get_dataset`, and `run_analysis` all accept either.

Every `list_*` method returns a `list` subclass
(`cirro.sdk.asset.DataPortalAssets`) with extra lookup helpers:
`get_by_name`, `get_by_id`, and `filter_by_pattern`.

## Freshness

These objects hold a snapshot of what the API returned when they were built.
Properties such as `cirro.sdk.dataset.DataPortalDataset.status` and
`cirro.sdk.dataset.DataPortalDataset.logs` will not change on an object you
already have. To watch a running analysis, call `portal.get_dataset(...)` again
each time round the loop.

## Worked examples

The [samples directory](https://github.com/CirroBio/Cirro-SDK-Python/tree/main/samples)
holds runnable notebooks for uploading, downloading, reading files, running and
debugging analyses, managing reference data, and integrating pipelines.
"""

import cirro.file_utils  # noqa
from cirro.cirro_client import CirroApi
from cirro.sdk.dataset import DataPortalDataset
from cirro.sdk.file import DataPortalFile
from cirro.sdk.login import DataPortalLogin
from cirro.sdk.portal import DataPortal
from cirro.sdk.process import DataPortalProcess
from cirro.sdk.project import DataPortalProject
from cirro.sdk.reference import DataPortalReference

__all__ = [
    'DataPortal',
    'DataPortalLogin',
    'DataPortalProject',
    'DataPortalProcess',
    'DataPortalDataset',
    'DataPortalReference',
    'DataPortalFile',
    'CirroApi',
    'file_utils'
]
