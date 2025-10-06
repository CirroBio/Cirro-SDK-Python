import importlib

import click
import requests
from cirro_api_client.v1.errors import CirroException

from cirro.cli import run_create_pipeline_config, run_validate_folder
from cirro.cli import run_ingest, run_download, run_configure, run_list_datasets
from cirro.cli.controller import handle_error, run_upload_reference
from cirro.cli.interactive.utils import InputError


def check_required_args(args):
    if args.get('interactive'):
        return
    if any(value is None for value in args.values()):
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
        ctx.exit()


@click.group(help="Cirro CLI - Tool for interacting with datasets")
@click.version_option()
def run():
    pass  # Print out help text, nothing to do


@run.command(help='List datasets', no_args_is_help=True)
@click.option('--project',
              help='Name or ID of the project')
@click.option('-i', '--interactive',
              help='Gather arguments interactively',
              is_flag=True, default=False)
def list_datasets(**kwargs):
    check_required_args(kwargs)
    run_list_datasets(kwargs, interactive=kwargs.get('interactive'))


@run.command(help='Download dataset files', no_args_is_help=True)
@click.option('--project',
              help='Name or ID of the project')
@click.option('--dataset',
              help='ID of the dataset')
@click.option('--file',
              help='Relative path of the file(s) to download (optional, can be used multiple times)',
              default=[],
              multiple=True)
@click.option('--data-directory',
              help='Directory to store the files')
@click.option('-i', '--interactive',
              help='Gather arguments interactively',
              is_flag=True, default=False)
def download(**kwargs):
    check_required_args(kwargs)
    run_download(kwargs, interactive=kwargs.get('interactive'))


@run.command(help='Upload and create a dataset', no_args_is_help=True)
@click.option('--name',
              help='Name of the dataset')
@click.option('--description',
              help='Description of the dataset (optional)',
              default='')
@click.option('--project',
              help='Name or ID of the project')
@click.option('--data-type', '--process',
              help='Name or ID of the data type (--process is deprecated)')
@click.option('--data-directory',
              help='Directory you wish to upload')
@click.option('--file',
              help='Relative path of the file(s) to upload (optional, can be used multiple times)',
              default=[],
              multiple=True)
@click.option('-i', '--interactive',
              help='Gather arguments interactively',
              is_flag=True, default=False)
@click.option('--include-hidden',
              help='Include hidden files in the upload (e.g., files starting with .)',
              is_flag=True, default=False)
def upload(**kwargs):
    check_required_args(kwargs)
    run_ingest(kwargs, interactive=kwargs.get('interactive'))


@run.command(help='Validate a dataset exactly matches a local folder', no_args_is_help=True)
@click.option('--dataset',
              help='Name or ID of the dataset')
@click.option('--project',
              help='Name or ID of the project')
@click.option('--data-directory',
              help='Local directory you wish to validate')
@click.option('-i', '--interactive',
              help='Gather arguments interactively',
              is_flag=True, default=False)
def validate(**kwargs):
    check_required_args(kwargs)
    run_validate_folder(kwargs, interactive=kwargs.get('interactive'))


@run.command(help='Upload a reference to a project', no_args_is_help=True)
@click.option('--name',
              help='Name of the reference')
@click.option('--reference-type',
              help='Type of the reference (e.g., Reference Genome (FASTA))')
@click.option('--project',
              help='Name or ID of the project')
@click.option('--reference-file',
              help='Location of reference file(s) to upload (can be used multiple times)',
              multiple=True)
@click.option('-i', '--interactive',
              help='Gather arguments interactively',
              is_flag=True, default=False)
def upload_reference(**kwargs):
    check_required_args(kwargs)
    run_upload_reference(kwargs, interactive=kwargs.get('interactive'))


@run.command(help='Configure authentication')
def configure():
    run_configure()


@run.command(help='Create pipeline configuration files')
@click.option('-p', '--pipeline-dir',
              metavar='DIRECTORY',
              help='Directory containing the pipeline definition files (e.g., WDL or Nextflow)',
              default='.',
              show_default=True)
@click.option('-e', '--entrypoint',
              help=(
                  'Entrypoint WDL file (optional, if not specified, the first WDL file found will be used).'
                  ' Ignored for Nextflow pipelines.'),
              default='main.wdl')
@click.option('-o', '--output-dir',
              help='Directory to store the generated configuration files',
              default='.cirro',
              show_default=True)
@click.option('-i', '--interactive',
              help='Gather arguments interactively',
              is_flag=True, default=False)
def create_pipeline_config(**kwargs):
    check_required_args(kwargs)
    run_create_pipeline_config(kwargs, interactive=kwargs.get('interactive'))


def _check_version():
    """
    Prompts the user to update their package version if needed
    """
    yellow_color = '\033[93m'
    reset_color = '\033[0m'

    try:
        current_version = importlib.metadata.version('cirro')
        response = requests.get("https://pypi.org/pypi/cirro/json")
        response.raise_for_status()
        latest_version = response.json()["info"]["version"]

        if current_version != latest_version:
            print(f"{yellow_color}Warning:{reset_color} Cirro version {current_version} "
                  f"is out of date. Update to {latest_version} with 'pip install cirro --upgrade'.")

    except Exception:
        return


def main():
    try:
        _check_version()
        run()
    except InputError as e:
        handle_error(e)
    except CirroException as e:
        handle_error(e)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
