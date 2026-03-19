import io
import json
import pickle
import unittest
from unittest.mock import Mock

import pandas as pd

from cirro.models.file import File, FileAccessContext
from cirro.sdk.dataset import DataPortalDataset, _infer_file_format, _read_file_with_format, _pattern_to_captures_regex
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

    def test_json_extension(self):
        self.assertEqual(_infer_file_format('data/results.json'), 'json')

    def test_json_gz_extension(self):
        self.assertEqual(_infer_file_format('data/results.json.gz'), 'json')

    def test_parquet_extension(self):
        self.assertEqual(_infer_file_format('data/results.parquet'), 'parquet')

    def test_feather_extension(self):
        self.assertEqual(_infer_file_format('data/results.feather'), 'feather')

    def test_pickle_pkl_extension(self):
        self.assertEqual(_infer_file_format('data/results.pkl'), 'pickle')

    def test_pickle_pickle_extension(self):
        self.assertEqual(_infer_file_format('data/results.pickle'), 'pickle')

    def test_excel_xlsx_extension(self):
        self.assertEqual(_infer_file_format('data/results.xlsx'), 'excel')

    def test_excel_xls_extension(self):
        self.assertEqual(_infer_file_format('data/results.xls'), 'excel')

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
            _read_file_with_format(self.file, 'xyz_unknown')

    def test_json_format(self):
        file = _make_mock_file('data/data.json', b'{"key": "value"}')
        result = _read_file_with_format(file, 'json')
        self.assertIsInstance(result, dict)
        self.assertEqual(result['key'], 'value')

    def test_auto_infer_json(self):
        file = _make_mock_file('data/data.json', b'[1, 2, 3]')
        result = _read_file_with_format(file, None)
        self.assertIsInstance(result, list)
        self.assertEqual(result, [1, 2, 3])

    def test_pickle_format(self):
        data = {'hello': 42}
        file = _make_mock_file('data/data.pkl', pickle.dumps(data))
        result = _read_file_with_format(file, 'pickle')
        self.assertEqual(result, data)

    def test_auto_infer_pickle(self):
        data = [1, 2, 3]
        file = _make_mock_file('data/data.pkl', pickle.dumps(data))
        result = _read_file_with_format(file, None)
        self.assertEqual(result, data)

    def _make_parquet_bytes(self):
        buf = io.BytesIO()
        pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).to_parquet(buf)
        return buf.getvalue()

    def _make_feather_bytes(self):
        buf = io.BytesIO()
        pd.DataFrame({'a': [1, 2], 'b': [3, 4]}).to_feather(buf)
        return buf.getvalue()

    @unittest.skipUnless(
        __import__('importlib').util.find_spec('pyarrow') is not None,
        'pyarrow not installed'
    )
    def test_parquet_format(self):
        file = _make_mock_file('data/data.parquet', self._make_parquet_bytes())
        result = _read_file_with_format(file, 'parquet')
        self.assertIsInstance(result, pd.DataFrame)
        self.assertListEqual(list(result.columns), ['a', 'b'])

    @unittest.skipUnless(
        __import__('importlib').util.find_spec('pyarrow') is not None,
        'pyarrow not installed'
    )
    def test_auto_infer_parquet(self):
        file = _make_mock_file('data/data.parquet', self._make_parquet_bytes())
        result = _read_file_with_format(file, None)
        self.assertIsInstance(result, pd.DataFrame)

    @unittest.skipUnless(
        __import__('importlib').util.find_spec('pyarrow') is not None,
        'pyarrow not installed'
    )
    def test_feather_format(self):
        file = _make_mock_file('data/data.feather', self._make_feather_bytes())
        result = _read_file_with_format(file, 'feather')
        self.assertIsInstance(result, pd.DataFrame)
        self.assertListEqual(list(result.columns), ['a', 'b'])

    @unittest.skipUnless(
        __import__('importlib').util.find_spec('pyarrow') is not None,
        'pyarrow not installed'
    )
    def test_auto_infer_feather(self):
        file = _make_mock_file('data/data.feather', self._make_feather_bytes())
        result = _read_file_with_format(file, None)
        self.assertIsInstance(result, pd.DataFrame)

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

    # --- glob mode ---

    def test_glob_matches_csv(self):
        results = list(self.dataset.read_files(glob='*.csv'))
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], pd.DataFrame)

    def test_glob_matches_multiple(self):
        results = list(self.dataset.read_files(glob='data/*'))
        self.assertEqual(len(results), 2)

    def test_glob_no_match_returns_empty(self):
        results = list(self.dataset.read_files(glob='*.parquet'))
        self.assertEqual(len(results), 0)

    def test_glob_explicit_format_csv(self):
        results = list(self.dataset.read_files(glob='data/*.tsv', filetype='csv', sep='\t'))
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], pd.DataFrame)
        self.assertIn('gene', results[0].columns)

    def test_glob_explicit_format_text(self):
        results = list(self.dataset.read_files(glob='logs/*.log', filetype='text'))
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], str)
        self.assertIn('started', results[0])

    def test_glob_auto_infer_csv_from_extension(self):
        results = list(self.dataset.read_files(glob='data/results.csv'))
        self.assertIsInstance(results[0], pd.DataFrame)

    def test_glob_auto_infer_text_from_extension(self):
        results = list(self.dataset.read_files(glob='logs/run.log'))
        self.assertIsInstance(results[0], str)

    def test_globstar_pattern(self):
        results = list(self.dataset.read_files(glob='**/*.csv'))
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], pd.DataFrame)

    # --- pattern (capture) mode ---

    def test_pattern_simple_filename(self):
        results = list(self.dataset.read_files(pattern='{sample}.csv'))
        self.assertEqual(len(results), 1)
        content, captures = results[0]
        self.assertIsInstance(content, pd.DataFrame)
        self.assertEqual(captures['sample'], 'results')

    def test_pattern_with_directory(self):
        results = list(self.dataset.read_files(pattern='data/{sample}.csv'))
        self.assertEqual(len(results), 1)
        _, captures = results[0]
        self.assertEqual(captures['sample'], 'results')

    def test_pattern_multiple_files(self):
        dataset = _make_dataset_with_files([
            _make_mock_file('sampleA.csv', b'a\n1\n'),
            _make_mock_file('sampleB.csv', b'a\n2\n'),
            _make_mock_file('notes.txt', b'text'),
        ])
        results = list(dataset.read_files(pattern='{sample}.csv'))
        self.assertEqual(len(results), 2)
        captured = {c['sample'] for _, c in results}
        self.assertSetEqual(captured, {'sampleA', 'sampleB'})

    def test_pattern_multi_level(self):
        dataset = _make_dataset_with_files([
            _make_mock_file('treated/sampleA.csv', b'x\n1\n'),
            _make_mock_file('control/sampleB.csv', b'x\n2\n'),
        ])
        results = list(dataset.read_files(pattern='{condition}/{sample}.csv'))
        self.assertEqual(len(results), 2)
        by_sample = {c['sample']: c['condition'] for _, c in results}
        self.assertEqual(by_sample['sampleA'], 'treated')
        self.assertEqual(by_sample['sampleB'], 'control')

    def test_pattern_no_match_returns_empty(self):
        results = list(self.dataset.read_files(pattern='{sample}.parquet'))
        self.assertEqual(len(results), 0)

    def test_pattern_yields_content_and_captures_tuple(self):
        results = list(self.dataset.read_files(pattern='{sample}.csv'))
        content, captures = results[0]
        self.assertIsInstance(captures, dict)
        self.assertIn('sample', captures)

    # --- error cases ---

    def test_both_glob_and_pattern_raises(self):
        with self.assertRaises(DataPortalInputError):
            list(self.dataset.read_files(glob='*.csv', pattern='{sample}.csv'))

    def test_neither_glob_nor_pattern_raises(self):
        with self.assertRaises(DataPortalInputError):
            list(self.dataset.read_files())


class TestPatternToRegex(unittest.TestCase):
    def _match(self, pattern, path):
        compiled, names = _pattern_to_captures_regex(pattern)
        m = compiled.match(path)
        return m.groupdict() if m else None

    def test_simple_capture(self):
        self.assertEqual(self._match('{sample}.csv', 'sampleA.csv'), {'sample': 'sampleA'})

    def test_simple_capture_with_directory(self):
        self.assertEqual(self._match('{sample}.csv', 'data/sampleA.csv'), {'sample': 'sampleA'})

    def test_directory_capture(self):
        self.assertEqual(self._match('data/{sample}.csv', 'data/results.csv'), {'sample': 'results'})

    def test_multi_level_capture(self):
        result = self._match('{condition}/{sample}.csv', 'treated/sampleA.csv')
        self.assertEqual(result, {'condition': 'treated', 'sample': 'sampleA'})

    def test_multi_level_capture_with_prefix(self):
        result = self._match('{condition}/{sample}.csv', 'data/treated/sampleA.csv')
        self.assertEqual(result, {'condition': 'treated', 'sample': 'sampleA'})

    def test_no_match_returns_none(self):
        self.assertIsNone(self._match('{sample}.csv', 'sampleA.tsv'))

    def test_wildcard_mixed_with_capture(self):
        result = self._match('data/*/{sample}.csv', 'data/subdir/sampleA.csv')
        self.assertEqual(result, {'sample': 'sampleA'})

    def test_capture_names_returned(self):
        _, names = _pattern_to_captures_regex('{condition}/{sample}.csv')
        self.assertListEqual(names, ['condition', 'sample'])
