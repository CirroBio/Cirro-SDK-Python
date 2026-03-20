# Cirro Client SDK — Claude Code Guide

## Overview

The `cirro` package provides access to the **Cirro Data Portal**, a cloud-based platform for managing bioinformatics datasets, running analysis pipelines, and organizing scientific data. It ships two interfaces:

- **Python SDK** — import as a library; designed for notebooks, scripts, and programmatic workflows
- **CLI** — the `cirro` command; designed for shell scripts, one-off operations, and interactive terminal use

**Install:** `pip install cirro`

---

## CLI vs Python SDK — When to Use Each

| Task | CLI | Python SDK |
|------|-----|-----------|
| First-time setup / authentication | `cirro configure` | — |
| Download a dataset to disk (one-off) | `cirro download` | `dataset.download_files()` |
| Upload raw data files | `cirro upload` | `project.upload_dataset()` |
| Validate a local folder against a dataset | `cirro validate` | `cirro.datasets.validate_folder()` |
| Browse datasets interactively in a terminal | `cirro list-datasets -i` | — |
| Create pipeline config from WDL/Nextflow | `cirro create-pipeline-config` | — |
| Read files into pandas/AnnData without downloading | — | `file.read_csv()`, `file.read_h5ad()` |
| Exploratory data analysis in a notebook | — | `DataPortal`, `DataPortalDataset` |
| Run analyses programmatically or in loops | — | `dataset.run_analysis()` |
| Filter/join metadata across many datasets | — | Python SDK (full pandas/numpy available) |
| Automate workflows in CI/CD | CLI (shell scripts) | SDK (Python scripts with credentials) |

**The core distinction:** use the CLI for moving data in and out of Cirro from the command line; use the Python SDK when you need to work with the data itself — reading, transforming, visualizing, or running analyses programmatically.

---

## Authentication and Connection

> **Institution-specific URL:** The `base_url` identifies your organization's dedicated Cirro environment. Every institution, company, or research group that runs Cirro has its own URL (e.g. `myorg.cirro.bio`). Ask your Cirro administrator for the correct URL. Once set via `cirro configure` or saved in `~/.cirro/config.ini`, you don't need to pass it explicitly in code.

### Interactive (Device Code — default for notebooks)

```python
from cirro import DataPortal

portal = DataPortal(base_url="<your-org>.cirro.bio")
# Opens browser for login, blocks until complete
```

### Non-blocking (show login link in UI)

```python
from cirro import DataPortalLogin

login = DataPortalLogin(base_url="<your-org>.cirro.bio")
print(login.auth_message)          # Show user the login URL
portal = login.await_completion()  # Block until done
```

### Service-to-service (Client Credentials)

```python
import os
from cirro import CirroApi
from cirro.auth.client_creds import ClientCredentialsAuth
from cirro.config import AppConfig

config = AppConfig(base_url="<your-org>.cirro.bio")
auth = ClientCredentialsAuth(
    client_id=os.environ["CIRRO_CLIENT_ID"],
    client_secret=os.environ["CIRRO_CLIENT_SECRET"],
    auth_endpoint=config.auth_endpoint
)
cirro = CirroApi(auth_info=auth)
```

### Configuration file (`~/.cirro/config.ini`)

```ini
[General]
base_url = <your-org>.cirro.bio   # Set by `cirro configure`; varies per institution

[DeviceCodeAuth]
enable_cache = True   # Cache token between sessions
```

---

## Project and Dataset Discovery

### List and browse projects

```python
projects = portal.list_projects()
print(projects)                          # Pretty-printed table

project = portal.get_project("My Project")           # by name or ID
project = portal.get_project_by_name("My Project")   # by name
project = portal.get_project_by_id("proj-uuid")      # by ID
```

### List and filter datasets

```python
datasets = project.list_datasets()
print(datasets)                              # Pretty-printed table

dataset = project.get_dataset_by_name("RNA-seq Batch 1")
dataset = project.get_dataset_by_id("ds-uuid")

# Filter by name pattern
rna_datasets = datasets.filter_by_pattern("RNA*")
```

### Useful dataset properties

```python
dataset.id            # UUID
dataset.name          # Display name
dataset.description
dataset.status        # e.g. "COMPLETED", "RUNNING"
dataset.process_id    # The data type or pipeline that created it
dataset.params        # Dict of parameters used
dataset.tags          # List of tag strings
dataset.created_at    # Timestamp
dataset.created_by    # Username
dataset.source_dataset_ids  # Input datasets (for analysis results)
```

---

## Working with Files

### List and access files

```python
files = dataset.list_files()
print(files)                                    # Pretty-printed table

file = dataset.get_file("results/summary.csv")  # by relative path
file = files.get_by_name("summary.csv")
```

### File properties

```python
file.name           # Filename
file.relative_path  # Path within dataset
file.absolute_path  # Full S3 path
file.size           # Human-readable size
file.size_bytes     # Bytes
```

### Read files into memory (no download needed)

```python
# Read CSV/TSV into pandas DataFrame
df = file.read_csv()
df = file.read_csv(sep="\t")            # TSV
df = file.read_csv(index_col=0)         # With index
df = file.read_csv(compression="gzip")  # Compressed

# Read AnnData (h5ad) — for single-cell data
adata = file.read_h5ad()

# Read raw text
content = file.read()         # Full string
lines = file.readlines()      # List of lines

# Read as bytes (e.g., for PIL, custom parsers)
bio = file.read_bytes()       # Returns BytesIO
```

### Download files

```python
# Download a single file
path = file.download("~/Downloads")

# Download all files in a dataset
dataset.download_files("~/Downloads")

# Download a filtered set
files.download("~/Downloads")
```

---

## Common EDA Patterns

### Quick data inspection

```python
# Get a project and browse its datasets
portal = DataPortal(base_url="<your-org>.cirro.bio")
project = portal.get_project("My Project")

for dataset in project.list_datasets():
    print(dataset.name, dataset.status, dataset.created_at)
```

### Load a results table

```python
dataset = project.get_dataset_by_name("DESeq2 Results")
results_file = dataset.get_file("results/differential_expression.csv")
de_df = results_file.read_csv(index_col=0)
de_df.head()
```

### Load single-cell AnnData

```python
dataset = project.get_dataset_by_name("Seurat Analysis")
h5ad_file = dataset.get_file("output/adata.h5ad")
adata = h5ad_file.read_h5ad()
print(adata)
```

### Get sample metadata

```python
samples = project.samples()         # Returns list of Sample objects
import pandas as pd
meta_df = pd.DataFrame([s.to_dict() for s in samples])
```

### Explore dataset artifacts (summary outputs)

```python
artifacts = dataset.list_artifacts()
for artifact in artifacts:
    print(artifact.name, artifact.relative_path)
```

---

## Uploading Data

### Upload a dataset

```python
project = portal.get_project("My Project")

dataset = project.upload_dataset(
    name="RNA-seq Batch 2",
    description="Second batch from patient cohort",
    process="Paired-end FASTQ",        # Data type name
    upload_folder="~/data/batch2",     # Local folder
    files=["sample1_R1.fastq.gz", "sample1_R2.fastq.gz"],  # Relative to folder
    tags=["batch2", "RNA-seq"]
)
```

### Update a samplesheet

```python
dataset.update_samplesheet(file_path="samplesheet.csv")
# or from string contents:
dataset.update_samplesheet(contents="sample,file\ns1,file.fastq.gz")
```

---

## Running Analyses

### List available pipelines

```python
processes = portal.list_processes(ingest=False)  # Analysis pipelines
print(processes)

process = portal.get_process_by_name("DESeq2")
print(process.description)
print(process.documentation_url)
```

### Get pipeline parameter schema

```python
param_spec = process.get_parameter_spec()
```

### Run an analysis

```python
result_dataset_id = dataset.run_analysis(
    name="DESeq2 Results — Batch 2",
    description="Differential expression analysis",
    process="DESeq2",                          # Pipeline name or ID
    params={
        "contrasts": "condition",
        "min_count": 10,
    },
    notifications_emails=["user@lab.edu"]
)

# Access the result dataset
result_dataset = project.get_dataset_by_id(result_dataset_id)
```

---

## Reference Data

```python
# List reference types
ref_types = portal.list_reference_types()

# List references for a type
refs = project.list_references(reference_type="Genome")
genome_ref = refs.get_by_name("GRCh38")

# Access reference files
for f in genome_ref.files:
    print(f.relative_path)
```

---

## Architecture Overview

```
DataPortal                  # Top-level entry point
  └── DataPortalProject     # A project (group of datasets)
        └── DataPortalDataset    # A dataset (collection of files)
              └── DataPortalFile      # A single file (readable/downloadable)

DataPortalProcess     # A data type or analysis pipeline
DataPortalReference   # Reference data (genomes, annotations)
```

All collection types (`DataPortalProjects`, `DataPortalDatasets`, etc.) extend Python `list` and support:
- `get_by_name(name)` — find by display name
- `get_by_id(id)` — find by UUID
- `filter_by_pattern(pattern)` — filter by glob pattern
- `print()` / `str()` — pretty-print as a table

---

## Key Imports

```python
from cirro import DataPortal               # High-level, recommended
from cirro import DataPortalLogin          # Non-blocking auth
from cirro import CirroApi                 # Low-level direct API access
from cirro.auth.client_creds import ClientCredentialsAuth  # Service auth
from cirro.config import AppConfig         # Configuration management
```

---

## Tips for Scientific Use

- **No download required**: `file.read_csv()`, `file.read_h5ad()`, `file.read()` stream directly into memory via S3.
- **Token caching**: Set `enable_cache = True` in `~/.cirro/config.ini` to avoid re-authenticating every session.
- **Lazy listing**: `list_datasets()` accepts `force_refresh=True` to bust the cache after uploads.
- **Chaining**: `portal.get_project("X").list_datasets().filter_by_pattern("RNA*")` is idiomatic.
- **Status check**: `dataset.status` is `"COMPLETED"` when results are ready; check before reading files.
- **Params inspection**: `dataset.params` contains the exact parameters used when the analysis ran — useful for reproducibility tracking.

---

## CLI Reference

The `cirro` command provides seven subcommands. Run `cirro --help` or `cirro <command> --help` for full option listings.

All data commands accept `-i` / `--interactive` to walk through options with guided prompts instead of flags.

### `cirro configure`

Set up authentication and save connection settings. Always run this first on a new machine.

```bash
cirro configure
# Prompts for Cirro URL, then opens browser for login
# Writes ~/.cirro/config.ini
```

After configuring, subsequent commands authenticate automatically. To re-authenticate, delete `~/.cirro/token.dat`.

---

### `cirro list-datasets`

Print a table of datasets in a project, sorted newest-first.

```bash
# Non-interactive
cirro list-datasets --project "My Project"

# Interactive (select project from menu)
cirro list-datasets -i
```

**Options:**
- `--project` — project name or ID

---

### `cirro download`

Download files from a dataset to a local directory. Validates checksums (CRC64) after transfer.

```bash
# Download all files
cirro download \
  --project "My Project" \
  --dataset "ds-uuid" \
  --data-directory ~/downloads

# Download specific files
cirro download \
  --project "My Project" \
  --dataset "ds-uuid" \
  --file "results/summary.csv" \
  --file "results/report.html" \
  --data-directory ~/downloads

# Interactive — select project, dataset, and files from menus
cirro download -i
```

**Options:**
- `--project` — project name or ID (required)
- `--dataset` — dataset ID (required)
- `--data-directory` — local destination directory (required)
- `--file` — specific file(s) to download; repeatable; downloads all if omitted

Interactive file selection offers three modes: all files, checkbox list, or glob pattern.

---

### `cirro upload`

Create a new dataset and upload files to it. Validates file contents against the data type's requirements before uploading.

```bash
# Upload all files in a directory
cirro upload \
  --project "My Project" \
  --name "RNA-seq Batch 2" \
  --data-type "Paired-end FASTQ" \
  --data-directory ~/data/batch2

# Upload specific files
cirro upload \
  --project "My Project" \
  --name "RNA-seq Batch 2" \
  --data-type "Paired-end FASTQ" \
  --data-directory ~/data/batch2 \
  --file "sample1_R1.fastq.gz" \
  --file "sample1_R2.fastq.gz"

# Include hidden files (e.g. .nf-config)
cirro upload ... --include-hidden

# Interactive
cirro upload -i
```

**Options:**
- `--project` — project name or ID (required)
- `--name` — dataset name (required)
- `--description` — dataset description (optional)
- `--data-type` — data type name or ID (required); `--process` is a deprecated alias
- `--data-directory` — local folder containing files (required)
- `--file` — specific file(s) to upload; repeatable; uploads all files if omitted
- `--include-hidden` — include dotfiles (default: excluded)

---

### `cirro validate`

Compare a local directory against a dataset's contents by checking checksums. Useful for verifying that a local copy is complete and uncorrupted.

```bash
cirro validate \
  --project "My Project" \
  --dataset "ds-uuid" \
  --data-directory ~/data/batch2

# Interactive
cirro validate -i
```

Output reports files grouped as: matched, checksum mismatches, missing locally, unexpected local files, or validation failed.

**Options:**
- `--project` — project name or ID (required)
- `--dataset` — dataset name or ID (required)
- `--data-directory` — local directory to compare (required)

---

### `cirro upload-reference`

Upload reference data (e.g. a genome FASTA and index) to a project. File names are validated against the reference type's requirements.

```bash
cirro upload-reference \
  --project "My Project" \
  --name "GRCh38" \
  --reference-type "Reference Genome (FASTA)" \
  --reference-file hg38.fa \
  --reference-file hg38.fa.fai

# Interactive (shows expected file patterns for each type)
cirro upload-reference -i
```

**Options:**
- `--project` — project name or ID (required)
- `--name` — reference name (required)
- `--reference-type` — type name (required)
- `--reference-file` — file path; repeatable (required)

---

### `cirro create-pipeline-config`

Generate Cirro pipeline configuration files (`process-form.json`, `process-input.json`) from a WDL or Nextflow pipeline definition. Used when registering a custom pipeline.

```bash
# From current directory (looks for main.wdl or nextflow_schema.json)
cirro create-pipeline-config

# Specify pipeline directory and output location
cirro create-pipeline-config \
  --pipeline-dir ~/my-pipeline \
  --output-dir .cirro

# Custom WDL entrypoint
cirro create-pipeline-config \
  --pipeline-dir ~/my-pipeline \
  --entrypoint workflow.wdl

# Interactive
cirro create-pipeline-config -i
```

**Options:**
- `--pipeline-dir` / `-p` — directory containing pipeline files (default: `.`)
- `--entrypoint` / `-e` — WDL entrypoint filename (default: `main.wdl`; ignored for Nextflow)
- `--output-dir` / `-o` — where to write generated JSON files (default: `.cirro`)

WDL pipelines should use v1.0+ with an explicit `input` block. Nextflow pipelines should include `nextflow_schema.json` (standard in NF-Core pipelines).

---

### Global CLI behavior

- **Version check**: every invocation checks PyPI for updates and warns if a newer version exists.
- **Error handling**: `InputError` and `CirroException` print an error message and exit with code 1; Ctrl-C exits silently.
- **Environment variables**: `CIRRO_BASE_URL` overrides the config file URL; `CIRRO_HOME` overrides `~/.cirro`.
