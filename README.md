# Cirro Client

[![Build Python package](https://github.com/FredHutch/Cirro-client/actions/workflows/package.yml/badge.svg)](https://github.com/FredHutch/Cirro-client/actions/workflows/package.yml)
[![Lint and run tests](https://github.com/FredHutch/Cirro-client/actions/workflows/lint.yml/badge.svg)](https://github.com/FredHutch/Cirro-client/actions/workflows/lint.yml)
![](https://img.shields.io/pypi/v/cirro.svg)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=CirroBio_Cirro-client&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=CirroBio_Cirro-client)

A Python 3.9+ library for the Cirro platform.

## Installation

You can install Cirro using pip:

`pip install cirro`

or you can install the main branch of the repo by running:

`pip install git+https://github.com/CirroBio/Cirro-client.git`

## Authentication

Upon first use, the Cirro client will ask you what Cirro instance to use and if you would like to save your login information.
It will then give you a link to authenticate through the web browser.

You can change your Cirro instance by running `cirro configure` and selecting the desired instance.

If you need to change your credentials after this point, and you've opted to save your login, please see the [clearing saved login](#clearing-saved-login) section.

## Command Line Usage

#### Downloading a dataset:

```bash
Usage: cirro download [OPTIONS]

  Download dataset files

Options:
  --project TEXT         Name or ID of the project
  --dataset TEXT         ID of the dataset
  --file TEXT            Relative path of the file(s) to download (optional, can be used multiple times)
  --data-directory TEXT  Directory to store the files
  -i, --interactive      Gather arguments interactively
  --help                 Show this message and exit.
```

```bash
$ cirro download --project "Test Project 1" --dataset "test" --data-directory "~/download"
```

#### Uploading a dataset:

```bash
Usage: cirro upload [OPTIONS]

  Upload and create a dataset

Options:
  --name TEXT                  Name of the dataset
  --description TEXT           Description of the dataset (optional)
  --project TEXT               Name or ID of the project
  --data-type, --process TEXT  Name or ID of the data type (--process is deprecated)
  --data-directory TEXT        Directory you wish to upload
  --file TEXT                  Relative path of the file(s) to upload (optional, can be used multiple times)
  -i, --interactive            Gather arguments interactively
  --include-hidden             Include hidden files in the upload (e.g., files starting with .)
  --help                       Show this message and exit.
```

```bash
$ cirro upload --project "Test Project 1" --name "test" --file "sample1.fastq.gz" --file "sample2.fastq.gz" --data-directory "~/data" --data-type "Paired DNAseq (FASTQ)" 
```

#### Uploading a reference

```bash
Usage: cirro upload-reference [OPTIONS]

  Upload a reference to a project

Options:
  --name TEXT            Name of the reference
  --reference-type TEXT  Type of the reference (e.g., Reference Genome (FASTA))
  --project TEXT         Name or ID of the project
  --reference-file TEXT  Location of reference file(s) to upload (can be used multiple times)
  -i, --interactive      Gather arguments interactively
  --help                 Show this message and exit.
```

#### Listing datasets:
```bash
Usage: cirro list-datasets [OPTIONS]

  List available datasets

Options:
  --project TEXT         ID of the project
  -i, --interactive      Gather arguments interactively
  --help                 Show this message and exit.
```

### Interactive Commands

When running a command, you can specify the `--interactive` flag to gather the command arguments interactively.

Example:

```bash
$ cirro upload --interactive
? What project is this dataset associated with?  Test project
? Enter the full path of the data directory  /shared/biodata/test
? Please confirm that you wish to upload 20 files (0.630 GB)  Yes
? What type of files?  Illumina Sequencing Run
? What is the name of this dataset?  test
? Enter a description of the dataset (optional)
```

## Python Usage

See the following set of Jupyter notebooks that contain examples on the following topics:

| Jupyter Notebook                                                   | Topic                                |
| ------------------------------------------------------------------ | ------------------------------------ |
| [Introduction](samples/Getting_started.ipynb)                      | Installing and authenticating        |
| [Uploading a dataset](samples/Uploading_a_dataset.ipynb)           | Uploading data                       |
| [Downloading a dataset](samples/Downloading_a_dataset.ipynb)       | Downloading data                     |
| [Interacting with a dataset](samples/Interacting_with_files.ipynb) | Calling data and reading into tables |
| [Analyzing a dataset](samples/Analyzing_a_dataset.ipynb)           | Running analysis pipelines           |
| [Using references](samples/Using_references.ipynb)                 | Managing reference data              |
| [Advanced usage](samples/Advanced_usage.ipynb)                     | Advanced operations                  |

## R Usage

| Jupyter Notebook                                    | Topic               |
| --------------------------------------------------- | ------------------- |
| [Downloading a dataset in R](samples/Using-R.ipynb) | Reading data with R |

## Advanced Usage

View the API documentation for this library [here](https://cirrobio.github.io/Cirro-client/).

### Supported environment variables

| Name           | Description                   | Default  |
| -------------- | ----------------------------- | -------- |
| CIRRO_HOME     | Local configuration directory | ~/.cirro |
| CIRRO_BASE_URL | Base URL of the data portal   |          |

### Configuration

The `cirro configure` command creates a file in `CIRRO_HOME` called `config.ini`.

You can set the `base_url` property in the config file rather than using the environment variable. 

The `transfer_max_retries` configuration property specifies the maximum number of times to attempt uploading a file to Cirro in the event of a transfer failure. 
When uploading files to Cirro, network issues or temporary outages can occasionally cause a transfer to fail.
It will pause for an increasing amount of time for each retry attempt.

The `enable_additional_checksums` property manages the utilization of SHA-256 hashing for enhanced data integrity. 
This feature computes the SHA-256 hash of a file during the upload process, and subsequently cross-validates it with the server upon completion.
When retrieving files, it ensures that the hash received matches the server's stored hash.
The default hashing algorithm for files is CRC64. In many cases, CRC64 is sufficient to ensure data integrity upon upload.

```ini
[General]
base_url = cirro.bio
transfer_max_retries = 15
enable_additional_checksums = true
```

### Clearing saved login

You can clear your saved login information by removing the `~/.cirro/token.dat` file from your system or
by running `cirro configure` and selecting **No** when it asks if you'd like to save your login information.
