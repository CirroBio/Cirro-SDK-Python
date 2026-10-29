import unittest
from unittest.mock import Mock

import pandas as pd
from cirro_api_client.v1.models import ProcessParameterSchema, FileFormat, CreateResponse

from cirro.models.cirro_uri import format_dataset_uri, parse_cirro_uri, find_dataset_ids
from cirro.models.form_specification import ParameterSpecification
from cirro.sdk.exceptions import DataPortalInputError
from cirro.sdk.process import DataPortalProcess

PROJECT_ID = 'project-1'

PARAMETER_SCHEMA = {
    'form': {
        'type': 'object',
        'required': ['input', 'genome'],
        'properties': {
            'input': {
                'type': 'string',
                'format': 'file-path',
                'schema': 'samplesheet',
                'title': 'Samplesheet'
            },
            'genome': {'type': 'string', 'enum': ['GRCh38', 'GRCm39']},
            'options': {
                'type': 'object',
                'properties': {
                    'contrasts': {'type': 'string', 'schema': 'contrasts'}
                }
            }
        }
    },
    'ui': {},
    'structuredInputs': {
        'samplesheet': {
            'fileFormat': 'CSV',
            'schema': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'required': ['sample', 'fastq_1'],
                    'properties': {
                        'sample': {'type': 'string'},
                        'fastq_1': {'type': 'string', 'format': 'file-path'},
                        'fastq_2': {'type': 'string', 'format': 'file-path'},
                        'replicate': {'type': 'integer'}
                    }
                }
            },
            'examples': [{'sample': 'a', 'fastq_1': 'cirro::p:dataset:d/a_R1.fastq.gz'}]
        },
        'contrasts': {
            'fileFormat': 'TSV',
            'schema': {
                'type': 'array',
                'items': {'type': 'object', 'properties': {'id': {'type': 'string'}}}
            }
        }
    }
}


def _spec() -> ParameterSpecification:
    return ParameterSpecification(ProcessParameterSchema.from_dict(PARAMETER_SCHEMA))


def _row(sample='s1', dataset_id='dataset-1', **cells):
    return {'sample': sample, 'fastq_1': format_dataset_uri(PROJECT_ID, dataset_id, f'{sample}_R1.fastq.gz'), **cells}


class TestCirroUri(unittest.TestCase):
    def test_format_and_parse(self):
        uri = format_dataset_uri(PROJECT_ID, 'dataset-1', 'data/folder/a.fastq.gz')
        self.assertEqual(uri, 'cirro::project-1:dataset:dataset-1/folder/a.fastq.gz')
        parsed = parse_cirro_uri(uri)
        self.assertEqual((parsed.project_id, parsed.id, parsed.path), (PROJECT_ID, 'dataset-1', 'folder/a.fastq.gz'))
        self.assertIsNone(parse_cirro_uri(format_dataset_uri(PROJECT_ID, 'dataset-1')).path)
        self.assertIsNone(parse_cirro_uri('s3://bucket/datasets/dataset-1/data/a.fastq.gz'))

    def test_find_dataset_ids(self):
        value = {'a': [_row(dataset_id='d1'), _row(dataset_id='d2'), _row(dataset_id='d1')],
                 'b': format_dataset_uri('other-project', 'd3', '')}
        self.assertEqual(find_dataset_ids(value), ['d1', 'd2', 'd3'])
        self.assertEqual(find_dataset_ids(value, PROJECT_ID), ['d1', 'd2'])


class TestStructuredInputSpecification(unittest.TestCase):
    def test_parses_structured_inputs(self):
        spec = _spec()
        samplesheet = spec.structured_inputs['samplesheet']
        self.assertEqual(samplesheet.file_format, FileFormat.CSV)
        self.assertEqual(samplesheet.field_path, 'input')
        self.assertTrue(samplesheet.required)
        self.assertEqual([c.key for c in samplesheet.columns], ['sample', 'fastq_1', 'fastq_2', 'replicate'])
        self.assertEqual([c.required for c in samplesheet.columns], [True, True, False, False])
        self.assertEqual(len(samplesheet.examples), 1)

        contrasts = spec.structured_inputs['contrasts']
        self.assertEqual(contrasts.field_path, 'options.contrasts')
        self.assertFalse(contrasts.required)

    def test_validate_params_skips_structured_input_fields(self):
        spec = _spec()
        spec.validate_params({'genome': 'GRCh38'}, {'samplesheet': [_row()]})
        with self.assertRaisesRegex(RuntimeError, 'genome'):
            spec.validate_params({'genome': 'hg19'}, {'samplesheet': [_row()]})

    def test_validate_params_requires_structured_inputs(self):
        with self.assertRaisesRegex(RuntimeError, 'required: samplesheet'):
            _spec().validate_params({'genome': 'GRCh38'})

    def test_validate_params_rejects_rows_in_params(self):
        with self.assertRaisesRegex(RuntimeError, r'structured_inputs\["samplesheet"\]'):
            _spec().validate_params({'genome': 'GRCh38', 'input': [_row()]})

    def test_rejects_unknown_structured_input(self):
        with self.assertRaisesRegex(RuntimeError, 'Unknown structured input'):
            _spec().build_structured_inputs({'samplesheet': [_row()], 'other': []})

    def test_validates_rows_against_schema(self):
        samplesheet = _spec().structured_inputs['samplesheet']
        with self.assertRaisesRegex(RuntimeError, r"\$\[1\].*'fastq_1' is a required property"):
            samplesheet.validate_rows([_row(), {'sample': 's2'}])
        with self.assertRaisesRegex(RuntimeError, 'is not of type'):
            samplesheet.validate_rows([_row(replicate='one')])

    def test_requires_cirro_uris_for_paths(self):
        samplesheet = _spec().structured_inputs['samplesheet']
        with self.assertRaisesRegex(RuntimeError, r'\$\[0\]\.fastq_1.*not a Cirro URI'):
            samplesheet.validate_rows([{'sample': 's1', 'fastq_1': 's3://bucket/a.fastq.gz'}])

    def test_accepts_dataframe(self):
        samplesheet = _spec().structured_inputs['samplesheet']
        df = pd.DataFrame([_row('s1', replicate=1), _row('s2')])
        rows = samplesheet.validate_rows(df)
        # Empty cells are left out, rather than sent as null
        self.assertEqual(rows, [_row('s1', replicate=1), _row('s2')])

    def test_build_structured_inputs(self):
        request = _spec().build_structured_inputs({'samplesheet': [_row()]})
        self.assertEqual(request.to_dict(), {
            'samplesheet': {'data': [_row()], 'fileFormat': 'CSV', 'importSourceUris': []}
        })


class TestRunAnalysisStructuredInputs(unittest.TestCase):
    def _process(self):
        client = Mock()
        client.processes.get_parameter_spec.return_value = _spec()
        client.execution.run_analysis.return_value = CreateResponse(id='new-dataset', message='')
        process = Mock(id='process-1')
        return DataPortalProcess(process, client), client

    def test_sends_structured_inputs_and_their_datasets(self):
        process, client = self._process()
        dataset_id = process.run_analysis(
            name='Output',
            project_id=PROJECT_ID,
            params={'genome': 'GRCh38'},
            structured_inputs={'samplesheet': [_row('s1', 'd1'), _row('s2', 'd2')]}
        )
        self.assertEqual(dataset_id, 'new-dataset')
        request = client.execution.run_analysis.call_args.kwargs['request']
        self.assertEqual(request.source_dataset_ids, ['d1', 'd2'])
        self.assertEqual(request.to_dict()['structuredInputs']['samplesheet']['data'],
                         [_row('s1', 'd1'), _row('s2', 'd2')])

    def test_requires_datasets_without_references(self):
        process, client = self._process()
        client.processes.get_parameter_spec.return_value = ParameterSpecification(
            ProcessParameterSchema.from_dict({'form': {}, 'ui': {}})
        )
        with self.assertRaisesRegex(DataPortalInputError, 'datasets'):
            process.run_analysis(name='Output', project_id=PROJECT_ID)

    def test_invalid_rows_raise_input_error(self):
        process, _ = self._process()
        with self.assertRaisesRegex(DataPortalInputError, 'fastq_1'):
            process.run_analysis(name='Output', project_id=PROJECT_ID, params={'genome': 'GRCh38'},
                                 structured_inputs={'samplesheet': [{'sample': 's1'}]})
