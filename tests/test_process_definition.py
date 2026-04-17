import os
from os import path
import json
import unittest

from referencing import Resource

from cirro.models import process

DATA_PATH = path.join(path.dirname(__file__), 'data')


class TestPipelineDefinition(unittest.TestCase):
    def test_pipeline_definition_nextflow_with_schema(self):
        root_dir = path.join(DATA_PATH, 'workflows', 'nextflow', 'with-schema')
        pipeline = process.PipelineDefinition(root_dir)

        with open(path.join(root_dir, 'expected-form.json'), 'r') as f:
            expected_form_configuration = json.load(f)
            expected_parameter_schema = Resource.from_contents(expected_form_configuration['form'])
        
        with open(path.join(root_dir, 'expected-input.json'), 'r') as f:
            expected_input_configuration = json.load(f)
        
        self.assertEqual(pipeline.parameter_schema, expected_parameter_schema)
        self.assertEqual(pipeline.form_configuration, expected_form_configuration)
        self.assertEqual(pipeline.input_configuration, expected_input_configuration)
        self.assertEqual(pipeline.config_app_status, process.ConfigAppStatus.OPTIONAL)

    @unittest.skipIf(
        os.environ.get('CI') == 'true',
        "Skipping nf-core integration test in CI (requires Nextflow + version-specific schema output)"
    )
    def test_pipeline_definition_nextflow_without_schema(self):
        root_dir = path.join(DATA_PATH, 'workflows', 'nextflow', 'without-schema')
        pipeline = process.PipelineDefinition(root_dir)

        with open(path.join(root_dir, 'expected-form.json'), 'r') as f:
            expected_form_configuration = json.load(f)
            expected_parameter_schema = Resource.from_contents(expected_form_configuration['form'])
        
        with open(path.join(root_dir, 'expected-input.json'), 'r') as f:
            expected_input_configuration = json.load(f)
        
        self.assertEqual(pipeline.parameter_schema, expected_parameter_schema)
        self.assertEqual(pipeline.form_configuration, expected_form_configuration)
        self.assertEqual(pipeline.input_configuration, expected_input_configuration)
        self.assertEqual(pipeline.config_app_status, process.ConfigAppStatus.RECOMMENDED)

    def test_pipeline_definition_wdl_simple(self):
        root_dir = path.join(DATA_PATH, 'workflows', 'wdl', 'simple')
        pipeline = process.PipelineDefinition(root_dir)

        with open(path.join(root_dir, 'expected-form.json'), 'r') as f:
            expected_form_configuration = json.load(f)
            expected_parameter_schema = Resource.from_contents(expected_form_configuration['form'])
        
        with open(path.join(root_dir, 'expected-input.json'), 'r') as f:
            expected_input_configuration = json.load(f)
        
        self.assertEqual(pipeline.parameter_schema, expected_parameter_schema)
        self.assertEqual(pipeline.form_configuration, expected_form_configuration)
        self.assertEqual(pipeline.input_configuration, expected_input_configuration)
        self.assertEqual(pipeline.config_app_status, process.ConfigAppStatus.RECOMMENDED)

    def test_pipeline_definition_wdl_alt_entrypoint(self):
        root_dir = path.join(DATA_PATH, 'workflows', 'wdl', 'alt-entrypoint')
        pipeline = process.PipelineDefinition(root_dir, entrypoint='main-alt.wdl')

        with open(path.join(root_dir, 'expected-form.json'), 'r') as f:
            expected_form_configuration = json.load(f)
            expected_parameter_schema = Resource.from_contents(expected_form_configuration['form'])
        
        with open(path.join(root_dir, 'expected-input.json'), 'r') as f:
            expected_input_configuration = json.load(f)
        
        self.assertEqual(pipeline.parameter_schema, expected_parameter_schema)
        self.assertEqual(pipeline.form_configuration, expected_form_configuration)
        self.assertEqual(pipeline.input_configuration, expected_input_configuration)
        self.assertEqual(pipeline.config_app_status, process.ConfigAppStatus.RECOMMENDED)

    def test_get_input_params_simple(self):
        schema_path = path.join(DATA_PATH, 'jsonschemas', 'simple.json')
        with open(schema_path, 'r') as f:
            contents = json.load(f)
        
        schema = Resource.from_contents(contents)
        
        # Test with a simple property
        params = list(process.get_input_params('$', contents, schema))
        
        self.assertEqual(len(params), 3)
        self.assertEqual(params[0]['name'], 'param_1')
        self.assertEqual(params[0]['type'], 'string')
        self.assertEqual(params[0]['default'], None)
        self.assertEqual(params[0]['jsonPath'], '$.param_1')

    def test_get_input_params_nested(self):
        schema_path = path.join(DATA_PATH, 'jsonschemas', 'nested.json')
        with open(schema_path, 'r') as f:
            contents = json.load(f)
        
        schema = Resource.from_contents(contents)
        
        # Test with a nested property
        params = list(process.get_input_params('$', contents, schema))
        
        self.assertEqual(len(params), 3)
        self.assertEqual(params[0]['name'], 'param_1')
        self.assertEqual(params[0]['type'], 'string')
        self.assertEqual(params[0]['default'], None)
        self.assertEqual(params[0]['jsonPath'], '$.section_1.param_1')

        self.assertEqual(params[1]['name'], 'param_2')
        self.assertEqual(params[1]['type'], 'integer')
        self.assertEqual(params[1]['default'], 42)
        self.assertEqual(params[1]['jsonPath'], '$.param_2')


    def test_get_input_params_references(self):
        schema_path = path.join(DATA_PATH, 'jsonschemas', 'references.json')
        with open(schema_path, 'r') as f:
            contents = json.load(f)
        
        schema = Resource.from_contents(contents)
        
        # Test with a property that has a reference
        params = list(process.get_input_params('$', contents, schema))
        
        self.assertEqual(len(params), 3)
        self.assertEqual(params[0]['name'], 'param_1')
        self.assertEqual(params[0]['type'], 'string')
        self.assertEqual(params[0]['default'], None)
        self.assertEqual(params[0]['jsonPath'], '$.section_1.param_1')

        self.assertEqual(params[1]['name'], 'param_2')
        self.assertEqual(params[1]['type'], 'integer')
        self.assertEqual(params[1]['default'], 42)
        self.assertEqual(params[1]['jsonPath'], '$.param_2')


    def test_get_input_params_wdl_types(self):
        schema_path = path.join(DATA_PATH, 'jsonschemas', 'wdlType.json')
        with open(schema_path, 'r') as f:
            contents = json.load(f)
        
        schema = Resource.from_contents(contents)
        
        # Test with WDL types
        params = list(process.get_input_params('$', contents, schema))
        
        self.assertEqual(len(params), 4)
        self.assertEqual(params[0]['name'], 'file_param')
        self.assertEqual(params[0]['type'], 'string')
        self.assertEqual(params[0]['default'], None)
        self.assertEqual(params[0]['jsonPath'], '$.inputs[*].dataPath')

        self.assertEqual(params[1]['name'], 'directory_param')
        self.assertEqual(params[1]['type'], 'string')
        self.assertEqual(params[1]['default'], None)
        self.assertEqual(params[1]['jsonPath'], '$.inputs[*].dataPath')

        self.assertEqual(params[2]['name'], 'file_param_optional')
        self.assertEqual(params[2]['type'], 'string')
        self.assertEqual(params[2]['default'], None)
        self.assertEqual(params[2]['jsonPath'], '$.inputs[*].dataPath')

        self.assertEqual(params[3]['name'], 'directory_param_optional')
        self.assertEqual(params[3]['type'], 'string')
        self.assertEqual(params[3]['default'], None)
        self.assertEqual(params[3]['jsonPath'], '$.inputs[*].dataPath')


if __name__ == '__main__':
    unittest.main()