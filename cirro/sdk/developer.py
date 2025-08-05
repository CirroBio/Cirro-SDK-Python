from io import StringIO

from cirro_api_client.v1.api.datasets import get_sample_sheets, ingest_samples
from cirro_api_client.v1.api.processes import validate_file_name_patterns
from cirro_api_client.v1.models import SampleSheets, ValidateFileNamePatternsRequest, FileNameMatch

from cirro.cirro_client import CirroApi
from cirro.helpers import PreprocessDataset


class Matches(list[FileNameMatch]):
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
    """

    def __init__(self, client: CirroApi):
        self.client = client

    def generate_preprocess_for_input_datasets(self,
                                               project_id: str,
                                               input_dataset_ids: list[str],
                                               params=None) -> PreprocessDataset:
        """
        Generates a PreprocessDataset object for the given datasets

        With optional parameters to pass into the preprocess script.
        `metadata` is not available in this context, so it is mocked.
        """
        samplesheets = self._generate_samplesheets_for_datasets(project_id, input_dataset_ids)
        return PreprocessDataset(
            samplesheet=samplesheets.samples,
            files=samplesheets.files,
            params=params or {},
            # Mock metadata
            metadata={
                'dataset': {},
                'project': {},
                'inputs': [],
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
        """
        dataset_files = self.client.datasets.get_assets_listing(project_id=project_id, dataset_id=dataset_id).files
        file_names = [file.relative_path for file in dataset_files]
        return self.test_file_name_validation(file_names, file_name_patterns)

    def test_file_name_validation(self,
                                  file_names: list[str],
                                  file_name_patterns: list[str]) -> Matches:
        """
        Tests the file name validation for a list of file names against specified regex patterns.
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
        Generates Cirro samplesheets for a given dataset
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
