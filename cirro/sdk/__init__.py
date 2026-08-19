"""
The high-level, object-oriented interface to Cirro.

Start from `cirro.sdk.portal.DataPortal`, which is re-exported as
`cirro.DataPortal`. Every other class in this package is reached from it rather
than constructed directly:

```
DataPortal
├── list_projects()        -> DataPortalProject
│   ├── list_datasets()    -> DataPortalDataset
│   │   ├── list_files()   -> DataPortalFile
│   │   └── tasks          -> DataPortalTask
│   └── list_references()  -> DataPortalReference
├── list_processes()       -> DataPortalProcess
└── list_reference_types() -> DataPortalReferenceType
```

For the lower-level typed API client, see `cirro.cirro_client.CirroApi`.
"""
