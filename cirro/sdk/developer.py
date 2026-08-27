from io import StringIO

from cirro_api_client.v1.api.datasets import get_sample_sheets, ingest_samples
from cirro_api_client.v1.api.processes import validate_file_name_patterns
from cirro_api_client.v1.models import SampleSheets, ValidateFileNamePatternsRequest, FileNameMatch

from cirro.cirro_client import CirroApi
from cirro.helpers import PreprocessDataset


class Matches(list[FileNameMatch]):
    """
    The file name matches produced by
    `DeveloperHelper.test_file_name_validation`.
    """

    def print(self):
        """
        Prints the file name validation matches in a readable format.
        """
        print(f'Matches: {len(self)}')
        print()
        for match in self:
            print(f'{match.file_name}')
            print(f'Sample name: {match.sample_name}')
            print(f'Matched regex: {match.regex_pattern_match}')
            print()


class DeveloperHelper:
    """
    Helper class for developer-related tasks,
    such as adding samplesheet preprocessing for a pipeline
    or testing file name validation and sample autopopulation.

    These are for building and debugging Cirro pipelines and data types, not for
    analysing data. Obtained from
    `cirro.sdk.portal.DataPortal.developer_helper`.
    """

    def __init__(self, client: CirroApi):
        """
        Obtained from `cirro.sdk.portal.DataPortal.developer_helper`.

        ```python
        from cirro import DataPortal
        portal = DataPortal()
        helper = portal.developer_helper
        ```
        """
        self.client = client

    def generate_preprocess_for_input_datasets(self,
                                               project_id: str,
                                               input_dataset_ids: list[str],
                                               params=None) -> PreprocessDataset:
        """
        Generates a PreprocessDataset object for the given datasets.

        Use this to develop and test a pipeline's `preprocess.py` locally against
        real datasets, without running the pipeline.

        Args:
            project_id (str): ID of the project holding the input datasets.
            input_dataset_ids (list[str]): IDs of the datasets to use as input.
            params (dict): Parameters to expose to the preprocess script, as if
                they had been entered in the analysis form.

        Returns:
            `cirro.helpers.preprocess_dataset.PreprocessDataset` -- with real
            samplesheet and file listings, and partially mocked `metadata`: only
            `project` and `inputs` are populated, while `dataset` and `process`
            are empty.
        """
        samplesheets = self._generate_samplesheets_for_datasets(project_id, input_dataset_ids)
        project = self.client.projects.get(project_id)
        inputs = []
        for dataset_id in input_dataset_ids:
            input_dataset = self.client.datasets.get(project_id, dataset_id).to_dict()
            input_dataset['dataPath'] = input_dataset['s3'] + '/data'
            inputs.append(input_dataset)

        return PreprocessDataset(
            samplesheet=samplesheets.samples,
            files=samplesheets.files,
            params=params or {},
            # Mock metadata
            metadata={
                'dataset': {},
                'project': project.to_dict(),
                'inputs': inputs,
                'process': {}
            }
        )

    def test_file_name_validation_for_dataset(self,
                                              project_id: str,
                                              dataset_id: str,
                                              file_name_patterns: list[str]) -> Matches:
        """
        Tests the file name validation for a given dataset against specified regex patterns.

        Used when configuring Cirro's sample autopopulation feature.
        More info: https://docs.cirro.bio/features/samples/#using-auto-population

        Args:
            project_id (str): ID of the project holding the dataset.
            dataset_id (str): ID of the dataset whose file names to test against.
            file_name_patterns (list[str]): Regex patterns to test, as they would
                appear in a process definition.

        Returns:
            `Matches` -- call `print()` on it for a readable report of which
            files matched which pattern, and what sample name each produced.
        """
        dataset_files = self.client.datasets.get_assets_listing(project_id=project_id, dataset_id=dataset_id).files
        file_names = [file.relative_path for file in dataset_files]
        return self.test_file_name_validation(file_names, file_name_patterns)

    def test_file_name_validation(self,
                                  file_names: list[str],
                                  file_name_patterns: list[str]) -> Matches:
        """
        Tests the file name validation for a list of file names against specified regex patterns.

        The same as `test_file_name_validation_for_dataset`, but against file
        names you supply rather than a dataset's.

        Args:
            file_names (list[str]): File names to test.
            file_name_patterns (list[str]): Regex patterns to test them against.

        Returns:
            `Matches`
        """
        request_body = ValidateFileNamePatternsRequest(
            file_names=file_names,
            file_name_patterns=file_name_patterns
        )

        matches = validate_file_name_patterns.sync(
            process_id="test",
            body=request_body,
            client=self.client.api_client
        )
        return Matches(matches)

    def generate_samplesheets_for_dataset(self, project_id: str, dataset_id: str) -> SampleSheets:
        """
        Generates Cirro samplesheets for a given dataset.

        These are the `samplesheet.csv` and `files.csv` that Cirro stages for a
        pipeline run, useful for checking what a pipeline will actually receive.

        Args:
            project_id (str): ID of the project holding the dataset.
            dataset_id (str): ID of the dataset.

        Returns:
            `cirro_api_client.v1.models.SampleSheets` -- with `samples` and
            `files` each holding CSV text.
        """
        return get_sample_sheets.sync(
            project_id=project_id,
            dataset_id=dataset_id,
            client=self.client.api_client
        )

    def rerun_sample_ingest_for_dataset(self, project_id: str, dataset_id: str):
        """
        Reruns the sample ingest process for a given dataset.
        You'll want to do this if you have updated the file name patterns in your pipeline (or data type)

        This re-derives the dataset's samples and their metadata from its file
        names, replacing what is currently recorded.

        Args:
            project_id (str): ID of the project holding the dataset.
            dataset_id (str): ID of the dataset to re-ingest samples for.
        """
        ingest_samples.sync_detailed(
            project_id=project_id,
            dataset_id=dataset_id,
            client=self.client.api_client
        )

    def _generate_samplesheets_for_datasets(self, project_id: str, dataset_ids: list[str]) -> SampleSheets:
        """
        Generates Cirro samplesheets for multiple datasets in a project.
        """
        # Concatenate samplesheets using pandas
        import pandas
        samplesheets_dfs = []
        files_dfs = []
        for dataset_id in dataset_ids:
            samplesheet = self.generate_samplesheets_for_dataset(project_id, dataset_id)
            samplesheets_dfs.append(pandas.read_csv(StringIO(samplesheet.samples)))
            files_dfs.append(pandas.read_csv(StringIO(samplesheet.files)))

        samplesheets_df = pandas.concat(samplesheets_dfs, ignore_index=True)
        files_df = pandas.concat(files_dfs, ignore_index=True)
        return SampleSheets(
            samples=samplesheets_df.to_csv(index=False),
            files=files_df.to_csv(index=False)
        )
