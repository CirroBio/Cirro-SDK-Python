import unittest
from unittest.mock import Mock, patch, MagicMock

from cirro.models.file import File, FileAccessContext
from cirro.sdk.dataset import DataPortalDataset, _infer_file_format, _read_file_with_format
from cirro.sdk.exceptions import DataPortalInputError
from cirro.sdk.file import DataPortalFile, DataPortalFiles


def _make_mock_file(relative_path: str, content: bytes = b'') -> DataPortalFile:
    """Create a DataPortalFile with a mocked _get method."""
    access_context = Mock(spec=FileAccessContext)
    file = File(relative_path=relative_path, size=len(content), access_context=access_context)
    client = Mock()
    client.file.get_file.return_value = content
    portal_file = DataPortalFile(file=file, client=client)
    return portal_file


def _make_dataset_with_files(files: list) -> DataPortalDataset:
    """Create a DataPortalDataset whose list_files() returns the given DataPortalFile list."""
    dataset_data = Mock()
    dataset_data.id = 'ds-1'
    dataset_data.project_id = 'proj-1'
    dataset_data.name = 'Test Dataset'

    client = Mock()
    dataset = DataPortalDataset(dataset=dataset_data, client=client)
    dataset.list_files = Mock(return_value=DataPortalFiles(files))
    return dataset


class TestInferFileFormat(unittest.TestCase):
    def test_csv_extension(self):
        self.assertEqual(_infer_file_format('data/results.csv'), 'csv')

    def test_tsv_extension(self):
        self.assertEqual(_infer_file_format('data/results.tsv'), 'csv')

    def test_csv_gz_extension(self):
        self.assertEqual(_infer_file_format('data/results.csv.gz'), 'csv')

    def test_tsv_gz_extension(self):
        self.assertEqual(_infer_file_format('data/results.tsv.gz'), 'csv')

    def test_h5ad_extension(self):
        self.assertEqual(_infer_file_format('data/adata.h5ad'), 'h5ad')

    def test_text_fallback(self):
        self.assertEqual(_infer_file_format('data/notes.txt'), 'text')

    def test_log_fallback(self):
        self.assertEqual(_infer_file_format('logs/run.log'), 'text')

    def test_unknown_extension_fallback(self):
        self.assertEqual(_infer_file_format('data/file.xyz'), 'text')


class TestReadFileWithFormat(unittest.TestCase):
    def setUp(self):
        self.file = _make_mock_file('data/results.csv', b'a,b\n1,2\n')

    def test_csv_format(self):
        import pandas as pd
        df = _read_file_with_format(self.file, 'csv')
        self.assertIsInstance(df, pd.DataFrame)
        self.assertListEqual(list(df.columns), ['a', 'b'])

    def test_text_format(self):
        file = _make_mock_file('data/notes.txt', b'hello world')
        result = _read_file_with_format(file, 'text')
        self.assertEqual(result, 'hello world')

    def test_auto_infer_csv(self):
        import pandas as pd
        result = _read_file_with_format(self.file, None)
        self.assertIsInstance(result, pd.DataFrame)

    def test_auto_infer_text(self):
        file = _make_mock_file('data/notes.txt', b'hello')
        result = _read_file_with_format(file, None)
        self.assertIsInstance(result, str)

    def test_unsupported_format_raises(self):
        with self.assertRaises(DataPortalInputError):
            _read_file_with_format(self.file, 'parquet')

    def test_csv_kwargs_passed_through(self):
        import pandas as pd
        file = _make_mock_file('data/data.tsv', b'a\tb\n1\t2\n')
        df = _read_file_with_format(file, 'csv', sep='\t')
        self.assertIsInstance(df, pd.DataFrame)
        self.assertListEqual(list(df.columns), ['a', 'b'])


class TestDatasetReadFiles(unittest.TestCase):
    def setUp(self):
        self.csv_file = _make_mock_file('data/results.csv', b'x,y\n3,4\n')
        self.tsv_file = _make_mock_file('data/counts.tsv', b'gene\tcount\nTP53\t100\n')
        self.txt_file = _make_mock_file('logs/run.log', b'started\nfinished\n')
        self.dataset = _make_dataset_with_files([
            self.csv_file,
            self.tsv_file,
            self.txt_file,
        ])

    def test_pattern_matches_csv(self):
        results = list(self.dataset.read_files('*.csv'))
        self.assertEqual(len(results), 1)
        file, content = results[0]
        self.assertEqual(file.relative_path, 'data/results.csv')

    def test_pattern_matches_multiple(self):
        results = list(self.dataset.read_files('data/*'))
        self.assertEqual(len(results), 2)
        paths = {f.relative_path for f, _ in results}
        self.assertIn('data/results.csv', paths)
        self.assertIn('data/counts.tsv', paths)

    def test_pattern_no_match_returns_empty(self):
        results = list(self.dataset.read_files('*.parquet'))
        self.assertEqual(len(results), 0)

    def test_explicit_format_csv(self):
        import pandas as pd
        results = list(self.dataset.read_files('data/*.tsv', file_format='csv', sep='\t'))
        self.assertEqual(len(results), 1)
        _, df = results[0]
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn('gene', df.columns)

    def test_explicit_format_text(self):
        results = list(self.dataset.read_files('logs/*.log', file_format='text'))
        self.assertEqual(len(results), 1)
        _, content = results[0]
        self.assertIsInstance(content, str)
        self.assertIn('started', content)

    def test_auto_infer_csv_from_extension(self):
        import pandas as pd
        results = list(self.dataset.read_files('data/results.csv'))
        _, content = results[0]
        self.assertIsInstance(content, pd.DataFrame)

    def test_auto_infer_text_from_extension(self):
        results = list(self.dataset.read_files('logs/run.log'))
        _, content = results[0]
        self.assertIsInstance(content, str)

    def test_yields_file_and_content_tuples(self):
        results = list(self.dataset.read_files('data/*.csv'))
        self.assertEqual(len(results), 1)
        file, content = results[0]
        self.assertIsInstance(file, DataPortalFile)

    def test_globstar_pattern(self):
        results = list(self.dataset.read_files('**/*.csv'))
        self.assertEqual(len(results), 1)
        file, _ = results[0]
        self.assertEqual(file.relative_path, 'data/results.csv')
