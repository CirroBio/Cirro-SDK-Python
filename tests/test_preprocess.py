import os
import unittest
from pathlib import Path
from typing import NamedTuple, Optional

import pandas

from cirro.helpers.preprocess_dataset import PreprocessDataset

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
DATA_PATH = Path(__location__, 'data')
TEST_DATA_PATH = 's3://cirro-public-resources-20250818192017584600000001/test_data'


class PreprocessTestCase(NamedTuple):
    path: str
    name: str
    prefix: Optional[str] = 'fastq_'

    def preprocess_instance(self):
        example_data = DATA_PATH / self.path
        return PreprocessDataset(samplesheet=example_data / 'samplesheet.csv',
                                 files=example_data / 'files.csv',
                                 params={}, metadata={})

    def expected_df(self):
        example_data = DATA_PATH / self.path
        return pandas.read_csv(example_data / 'expected_wide.csv')


class TestPreprocess(unittest.TestCase):
    test_cases = [
        PreprocessTestCase(path='example_data_1', name='multi lane'),
        PreprocessTestCase(path='example_data_2', name='single lane'),
        PreprocessTestCase(path='example_data_3', name='no read data (not paired)', prefix='file_'),
    ]

    def test_pivot_samplesheet(self):
        for test_case in self.test_cases:
            with self.subTest(msg=test_case.name, i=test_case.path):
                ds = test_case.preprocess_instance()
                df = ds.pivot_samplesheet(column_prefix=test_case.prefix)

                expected_df = test_case.expected_df()

                self.assertEqual(
                    expected_df.sort_index(axis=1).to_csv(index=False),
                    df.sort_index(axis=1).to_csv(index=False)
                )

    def test_pivot_samplesheet_selected_metadata(self):
        test_case = self.test_cases[2]
        ds = test_case.preprocess_instance()
        df = ds.pivot_samplesheet(column_prefix=test_case.prefix,
                                  metadata_columns=['status'])

        self.assertEqual(['sample', 'file_1', 'file_2', 'status'],
                         df.columns.tolist())

    def test_pivot_samplesheet_filter_reads(self):
        test_case = PreprocessTestCase(path='example_data_4', name='with indexed reads')
        ds = test_case.preprocess_instance()
        df = ds.pivot_samplesheet(column_prefix=test_case.prefix,
                                  file_filter_predicate='readType == "R"')

        expected_df = test_case.expected_df()
        self.assertEqual(
            expected_df.sort_index(axis=1).to_csv(index=False),
            df.sort_index(axis=1).to_csv(index=False)
        )

    def test_wide_samplesheet_legacy(self):
        for test_case in self.test_cases:
            with self.subTest(msg=test_case.name, i=test_case.path):
                ds = test_case.preprocess_instance()
                df = ds.wide_samplesheet(column_prefix=test_case.prefix)

                expected_df = test_case.expected_df()

                # legacy wide_samplesheet does not include sample columns
                columns_to_remove = ds.samplesheet.columns.tolist()
                columns_to_remove.remove('sample')
                expected_df = expected_df.drop(columns=columns_to_remove, errors='ignore')
                self.assertEqual(
                    expected_df.sort_index(axis=1).to_csv(index=False),
                    df.sort_index(axis=1).to_csv(index=False)
                )

    def test_load_running(self):
        ds = PreprocessDataset.from_path(dataset_root=f'{TEST_DATA_PATH}/dataset1')

        self.assertGreater(len(ds.params), 0)
        self.assertGreater(len(ds.metadata), 0)
        self.assertGreater(ds.samplesheet.shape[0], 0)
        self.assertGreater(ds.files.shape[0], 0)

        df = ds.pivot_samplesheet()
        self.assertGreater(df.shape[0], 0)


if __name__ == '__main__':
    unittest.main()
