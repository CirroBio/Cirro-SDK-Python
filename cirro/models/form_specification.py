import json
from typing import TYPE_CHECKING, Dict, Any, List, Mapping, Optional, Tuple, Union

import jsonschema
from cirro_api_client.v1.models import FormSchema, ProcessParameterSchema, StructuredInputDefinition, \
    FileFormat, StructuredInput, StructuredInputDataItem, RunAnalysisRequestStructuredFileInputs

from cirro.models.cirro_uri import parse_cirro_uri

if TYPE_CHECKING:
    from pandas import DataFrame

# Rows for a structured input, as a list of dicts (one per row) or a pandas DataFrame
StructuredInputRows = Union[List[Dict[str, Any]], 'DataFrame']

# JSON schema formats whose values are references to a file or folder in Cirro
_PATH_FORMATS = {'file-path', 'file-path-pattern', 'directory-path', 'path'}


def _get_fields_in_schema(schema: Dict, parent_path='') -> List['Parameter']:
    # This breaks for more advanced json schema usages such as the if/then
    fields = []

    for [field_key, field_spec] in schema.items():
        field_path = field_key if parent_path == '' else f'{parent_path}.{field_key}'
        field = Parameter(field_key, field_spec, field_path)
        fields.append(field)

        if 'properties' in field_spec:
            fields.extend(_get_fields_in_schema(field_spec['properties'], field_path))

    return fields


def _is_structured_input_field(field_spec: Any) -> bool:
    """
    A form field is filled by a structured input when it names one
    (by its key in `structured_inputs`) in its own `schema` property
    """
    return isinstance(field_spec, dict) and 'schema' in field_spec


_SUBSCHEMA_ARRAY_KEYWORDS = ('allOf', 'anyOf', 'oneOf')
_SUBSCHEMA_KEYWORDS = ('then', 'else')


def _get_merged_subschemas(schema: Dict) -> List[Dict]:
    """
    Subschemas whose properties apply at the schema's own path rather than under a key of their own:
    the branches of allOf/anyOf/oneOf, then/else, and schema dependencies. They nest, so callers recurse.

    `if` is left out: its properties are match conditions, not fields.
    Array-valued dependencies only name properties to require, so declare no fields.
    """
    subschemas = [s for keyword in _SUBSCHEMA_ARRAY_KEYWORDS for s in (schema.get(keyword) or [])]
    subschemas.extend(schema[keyword] for keyword in _SUBSCHEMA_KEYWORDS if keyword in schema)
    subschemas.extend((schema.get('dependencies') or {}).values())
    return [s for s in subschemas if isinstance(s, dict)]


def _get_structured_input_fields(schema: Dict, parent_path='', conditional=False) -> List[Tuple[str, str, bool]]:
    """
    Every form field filled by a structured input, as (field path, structured input name, required).
    A field declared in a conditional branch is only required when that branch applies,
    so it is never reported as required.
    """
    fields = []
    required = set(schema.get('required') or [])
    for field_key, field_spec in (schema.get('properties') or {}).items():
        field_path = field_key if parent_path == '' else f'{parent_path}.{field_key}'
        if _is_structured_input_field(field_spec):
            fields.append((field_path, field_spec['schema'], not conditional and field_key in required))
        elif isinstance(field_spec, dict):
            fields.extend(_get_structured_input_fields(field_spec, field_path, conditional))
    for subschema in _get_merged_subschemas(schema):
        fields.extend(_get_structured_input_fields(subschema, parent_path, conditional=True))
    return fields


def _as_multiple_values(field_spec: Dict) -> Dict:
    """
    A field with `"multiple": true` (e.g. a path field selecting several files) takes an array of values,
    each conforming to the field as declared. A single value is still accepted, as before arrays were.
    """
    single = {key: value for key, value in field_spec.items() if key != 'multiple'}
    return {'anyOf': [single, {'type': 'array', 'items': single, 'uniqueItems': True}]}


def _normalize_multiple_values_fields(schema: Dict) -> Dict:
    """
    A copy of an object schema with its `multiple` fields (at any depth) taking arrays of values
    """
    normalized = dict(schema)
    if isinstance(schema.get('properties'), dict):
        normalized['properties'] = {
            key: _as_multiple_values(spec) if isinstance(spec, dict) and spec.get('multiple')
            else _normalize_multiple_values_fields(spec) if isinstance(spec, dict) else spec
            for key, spec in schema['properties'].items()
        }
    if isinstance(schema.get('items'), dict):
        normalized['items'] = _normalize_multiple_values_fields(schema['items'])
    for keyword in _SUBSCHEMA_ARRAY_KEYWORDS:
        if isinstance(schema.get(keyword), list):
            normalized[keyword] = [_normalize_multiple_values_fields(s) if isinstance(s, dict) else s
                                   for s in schema[keyword]]
    for keyword in _SUBSCHEMA_KEYWORDS:
        if isinstance(schema.get(keyword), dict):
            normalized[keyword] = _normalize_multiple_values_fields(schema[keyword])
    if isinstance(schema.get('dependencies'), dict):
        normalized['dependencies'] = {
            key: _normalize_multiple_values_fields(dep) if isinstance(dep, dict) else dep
            for key, dep in schema['dependencies'].items()
        }
    return normalized


def _without_structured_input_fields(schema: Dict, inherited_removed: frozenset = frozenset()) -> Dict:
    """
    A copy of the form's JSON schema without the fields filled by structured inputs,
    whose rows are passed separately from the parameters and validated against their own schema.
    Conditional branches are stripped too, since they can declare such fields, or require
    ones declared alongside them (`inherited_removed`).
    """
    stripped = dict(schema)
    properties = schema.get('properties')
    removed = set(inherited_removed)
    if isinstance(properties, dict):
        removed |= {key for key, spec in properties.items() if _is_structured_input_field(spec)}
        stripped['properties'] = {
            key: _without_structured_input_fields(spec) if isinstance(spec, dict) else spec
            for key, spec in properties.items()
            if key not in removed
        }
    if isinstance(schema.get('required'), list):
        stripped['required'] = [key for key in schema['required'] if key not in removed]

    # Branches apply at this same path, so they share the fields removed here
    removed = frozenset(removed)

    def strip(subschema):
        if isinstance(subschema, dict):
            return _without_structured_input_fields(subschema, removed)
        if isinstance(subschema, list):
            # Property dependency: names of properties to require
            return [key for key in subschema if key not in removed]
        return subschema

    for keyword in _SUBSCHEMA_ARRAY_KEYWORDS:
        if isinstance(schema.get(keyword), list):
            stripped[keyword] = [strip(s) for s in schema[keyword]]
    for keyword in _SUBSCHEMA_KEYWORDS:
        if keyword in schema:
            stripped[keyword] = strip(schema[keyword])
    if isinstance(schema.get('dependencies'), dict):
        stripped['dependencies'] = {key: strip(dep) for key, dep in schema['dependencies'].items()}
    return stripped


def _is_s3_uri(value: Any) -> bool:
    return isinstance(value, str) and value.startswith('s3://')


def _get_by_path(params: Mapping, path: str) -> Any:
    value = params
    for key in path.split('.'):
        if not isinstance(value, Mapping) or key not in value:
            return None
        value = value[key]
    return value


def _to_rows(rows: StructuredInputRows) -> List[Dict[str, Any]]:
    """
    Normalizes rows to a list of plain dicts, leaving out empty (None/NaN) cells
    """
    if hasattr(rows, 'to_json'):
        # A pandas DataFrame: round-trip through JSON for plain Python values with NaN as None
        rows = json.loads(rows.to_json(orient='records', date_format='iso'))
    if not isinstance(rows, list):
        raise TypeError(f'Structured input rows must be a list of dicts or a DataFrame, got {type(rows).__name__}')
    return [
        {key: value for key, value in dict(row).items() if value is not None}
        for row in rows
    ]


class StructuredInputSpecification:
    """
    Describes a structured input: a table of rows (e.g., a samplesheet) that a process
    takes as an input file. The rows are validated against `schema` and written out
    as `file_format` for the pipeline.
    """
    def __init__(self, name: str, definition: StructuredInputDefinition,
                 field_path: Optional[str] = None, required: bool = False):
        self.name = name
        """Key of the structured input, used in `run_analysis(structured_inputs=...)`"""
        self.file_format: FileFormat = definition.file_format
        """Format of the file passed to the pipeline (CSV, TSV, or JSON)"""
        self.schema: Dict = definition.schema.to_dict()
        """JSON schema of the rows (an array of objects)"""
        self.examples: List[Dict] = [e.to_dict() for e in definition.examples] \
            if isinstance(definition.examples, list) else []
        """Example rows conforming to the schema"""
        self.field_path = field_path
        """The form field this structured input fills, if any"""
        self.required = required
        """Whether rows must be provided to run the process"""

        items = self.schema.get('items') or {}
        required_columns = set(items.get('required') or [])
        self.columns: List[Parameter] = [
            Parameter(key, spec, key, required=key in required_columns)
            for key, spec in (items.get('properties') or {}).items()
        ]
        """Columns of each row"""

    def validate_rows(self, rows: StructuredInputRows) -> List[Dict[str, Any]]:
        """
        Validates that the given rows conform to the specification

        Args:
            rows: List of dicts (one per row) or a pandas DataFrame

        Returns:
            The rows, as a list of dicts with empty cells left out
        """
        rows = _to_rows(rows)
        try:
            jsonschema.validate(instance=rows, schema=_normalize_multiple_values_fields(self.schema))
        except jsonschema.ValidationError as e:
            raise RuntimeError(f'Structured input {self.name} at {e.json_path} error: {e.message}') from e

        # A file or folder cell must be absolute: a Cirro URI saying which dataset it's in, or an S3 URI.
        path_columns = [c.key for c in self.columns if c.spec.get('format') in _PATH_FORMATS]
        for index, row in enumerate(rows):
            for column in path_columns:
                value = row.get(column)
                # A `multiple` column holds a list of paths
                for path in value if isinstance(value, list) else [value]:
                    if path in (None, '') or _is_s3_uri(path) or parse_cirro_uri(path) is not None:
                        continue
                    raise RuntimeError(
                        f'Structured input {self.name} at $[{index}].{column} error: '
                        f'{path!r} is not a Cirro or S3 URI. '
                        'Reference files and folders in a dataset with cirro.models.cirro_uri.format_dataset_uri'
                    )
        return rows

    def to_request(self, rows: StructuredInputRows) -> StructuredInput:
        """
        Validates the rows and builds them into the structured input of a run analysis request
        """
        return StructuredInput(
            data=[StructuredInputDataItem.from_dict(row) for row in self.validate_rows(rows)],
            file_format=self.file_format,
            import_source_uris=[]
        )

    def print(self):
        """
        Prints out a user-friendly view of the structured input's columns
        """
        required = 'required' if self.required else 'optional'
        print(f"{self.name} ({self.file_format.value}, {required}):")
        for column in self.columns:
            print(f'\t{column}')


class ParameterSpecification:
    """
    Used to describe parameters used in a process (uses JSONSchema)
    """
    def __init__(self, form_schema: Union[ProcessParameterSchema, FormSchema]):
        self._form_spec_raw: Dict = form_schema.form.additional_properties
        self._form_spec_ui: Dict = form_schema.ui.additional_properties
        self._structured_input_raw: Dict = form_schema.structured_inputs.additional_properties
        self.form_spec = _get_fields_in_schema(self._form_spec_raw.get('properties') or {})

        fields = {name: (path, required)
                  for path, name, required in _get_structured_input_fields(self._form_spec_raw)}
        self.structured_inputs: Dict[str, StructuredInputSpecification] = {
            name: StructuredInputSpecification(name, definition, *fields.get(name, (None, False)))
            for name, definition in self._structured_input_raw.items()
        }
        """
        Structured inputs (e.g., samplesheets) the process takes, keyed by name.
        Pass their rows to `run_analysis` with `structured_inputs`, rather than in `params`.
        """

    @property
    def form_spec_json(self) -> dict:
        """
        Returns the JSON schema of the form specification
        """
        return self._form_spec_raw

    def validate_params(self, params: Dict, structured_inputs: Dict[str, StructuredInputRows] = None):
        """
        Validates that the given parameters (and structured inputs) conform to the specification

        Args:
            params (dict): Analysis parameters
            structured_inputs (dict): Rows for each structured input, keyed by its name
        """
        for spec in self.structured_inputs.values():
            if spec.field_path and isinstance(_get_by_path(params, spec.field_path), list):
                raise RuntimeError(f'Parameter {spec.field_path} is a structured input, '
                                   f'pass its rows in structured_inputs["{spec.name}"] instead of params')
        schema = _normalize_multiple_values_fields(_without_structured_input_fields(self._form_spec_raw))
        try:
            jsonschema.validate(instance=params, schema=schema)
        except jsonschema.ValidationError as e:
            raise RuntimeError(f'Parameter at {e.json_path} error: {e.message}') from e
        self.build_structured_inputs(structured_inputs)

    def build_structured_inputs(
            self,
            structured_inputs: Dict[str, StructuredInputRows] = None
    ) -> RunAnalysisRequestStructuredFileInputs:
        """
        Validates the rows of each structured input and builds them into the
        `structured_inputs` of a run analysis request

        Args:
            structured_inputs (dict): Rows for each structured input, keyed by its name
        """
        structured_inputs = structured_inputs or {}
        unknown = [name for name in structured_inputs if name not in self.structured_inputs]
        if unknown:
            accepted = ', '.join(self.structured_inputs) or 'none'
            raise RuntimeError(f'Unknown structured input(s): {", ".join(unknown)} (accepted: {accepted})')
        missing = [name for name, spec in self.structured_inputs.items()
                   if spec.required and name not in structured_inputs]
        if missing:
            raise RuntimeError(f'Structured input(s) required: {", ".join(missing)}')

        request = RunAnalysisRequestStructuredFileInputs()
        for name, rows in structured_inputs.items():
            request[name] = self.structured_inputs[name].to_request(rows)
        return request

    def print(self):
        """
        Prints out a user-friendly view of the parameters
        """
        print("Parameters:")
        for field in self.form_spec:
            tab_prefix = '\t' * (field.path.count('.') + 1)
            print(tab_prefix + str(field))
        if self.structured_inputs:
            print("Structured inputs:")
            for spec in self.structured_inputs.values():
                spec.print()


class Parameter:
    def __init__(self, key: str, spec: Dict[str, Any], path: str, required: bool = False):
        self.spec = spec
        self.key = key
        self.path = path
        self.required = required

    @property
    def is_group(self):
        return self.spec.get('type') == 'object'

    def __str__(self):
        display_value = ''
        additional_data = ''

        if self.spec.get('title'):
            display_value += f'{self.spec.get("title")}'
            additional_data += f'key={self.key}, '
        else:
            display_value += self.key

        if self.required:
            additional_data += 'required, '

        if structured_input := self.spec.get('schema'):
            additional_data += f'structured_input={structured_input}, '

        if self.spec.get('multiple'):
            additional_data += 'multiple, '

        for prop in ['default', 'type', 'format', 'enum', 'description']:
            if prop_value := self.spec.get(prop):
                additional_data += f'{prop}={prop_value}, '

        if additional_data and not self.is_group:
            display_value += f' ({additional_data.rstrip(", ")})'

        if self.is_group:
            display_value += ' (Group)'
        return display_value
