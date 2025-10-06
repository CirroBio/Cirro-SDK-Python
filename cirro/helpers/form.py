import json
import os
from collections import OrderedDict


class FormBuilder:
    """Build a form to drive inputs for running an analysis notebook non-interactively."""

    PARAM_TYPES = ['string', 'integer', 'boolean', 'number']

    def __init__(self):

        # Contents will be written out as the form
        self.form = dict(
            ui=OrderedDict(),
            form=OrderedDict(
                type="object",
                properties=OrderedDict()
            )
        )

        # Used to make sure that no keys are repeated
        self.used_keys = set()

        # Used to keep track when entering fields in a sub-section
        self.pointer = None

        # Store the params which will be populated either by:
        # a) While building the form, the optional `test_value` field will be used
        # b) While running non-interactively, it will use the values read from $PW_NOTEBOOK_DATA
        self.params = dict()

    def add_param(
        self,
        key: str = None,
        type: PARAM_TYPES = None,
        test_value=None,
        title: str = None,
        description: str = None,
        default=None,
        required: bool = None,
        multiple: bool = None
    ):
        """
        Add a parameter to the form.

        key:            (required) The unique identifier used to access the value provided by the user
        type:           (required) Variable type (options: string, integer, boolean, number)
        test_value:     (optional) The value which will be populated during interactive testing
        title:          (optional) Title describing the parameter
        description:    (optional) Longer description of the parameter
        default:        (optional) Default value shown to the user in the form
        required:       (optional) Whether or not the user must provide a value in the form
        multiple:       (optional) Whether or not the user may provide multiple values in the form
        """

        # Keys cannot be repeated
        assert key is not None, "Must specify key"
        assert key not in self.used_keys, f"Cannot repeat use of keys ('{key}')"
        self.used_keys.add(key)

        # Types must conform to the allowed values
        assert type is not None, "Must specify type"
        msg = f"Allowed values for type: '{', '.join(self.PARAM_TYPES)}', not '{type}'"
        assert type in self.PARAM_TYPES, msg

        # Start building the item in the form
        item = dict(type=type)

        # Populate the test value
        if test_value is not None:
            self.params[key] = test_value

        # Add the title
        if title is not None:
            item['title'] = title

        # Add the description
        if description is not None:
            item['description'] = description

        # Add the default
        if default is not None:
            item['default'] = default

        # Add the required
        if required is not None:
            item['required'] = required

        # Add the multiple
        if multiple is not None:
            item['multiple'] = multiple

        # Add the item to the form
        if self.pointer is None:
            self.form['form']['properties'][key] = item
        else:
            self.form['form']['properties'][self.pointer]['properties'][key] = item

    def add_section(self, title: str = None, description: str = None):
        """
        Add a section heading to the form.
        """

        # Name the section arbitrarily
        section_name = self._new_section_name()

        # Add new params to this section
        self.pointer = section_name

        # Add the section
        self.form['form']['properties'][section_name] = dict(
            title=title,
            description=description,
            properties=OrderedDict()
        )

    def _new_section_name(self):
        """Internal method to pick a new section name."""

        i = 1
        while f"section_{i}" in self.used_keys:
            i += 1
        self.used_keys.add(f"section_{i}")
        return f"section_{i}"

    def save(self):
        """
        When running interactively, save the form contents to notebook-form.json
        (or specify another path with $PW_NOTEBOOK_FORM).

        However, to support running non-interactively, if $PW_NOTEBOOK_DATA has been
        set to a local file, use those values to populate self.params instead.
        """

        # If running non-interactively, PW_NOTEBOOK_DATA will have been set
        if os.environ.get('PW_NOTEBOOK_DATA') is not None:

            notebook_data = os.environ.get('PW_NOTEBOOK_DATA')
            assert os.path.exists(notebook_data)

            self.params = {}

            # Read in the contents, and remove any section headings from the keys
            with open(notebook_data, 'r') as handle:
                for k, v in json.load(handle).items():
                    self.params[k.split(".")[-1]] = v

        # If running interactively, save out the form to notebook-form.json
        output_path = os.environ.get('PW_NOTEBOOK_FORM', 'notebook-form.json')
        with open(output_path, 'w') as handle:
            json.dump(self.form, handle, indent=4)
