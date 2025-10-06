from pathlib import Path
from typing import List, Union, Callable, TypeVar, Optional

import questionary
from prompt_toolkit.validation import Validator, ValidationError
from questionary import prompt


class InputError(Exception):
    """Errors detected from user input"""
    pass


class DirectoryValidator(Validator):
    def validate(self, document):
        is_a_directory = Path(document.text).expanduser().is_dir()
        if not is_a_directory or len(document.text.strip()) == 0:
            raise ValidationError(
                message='Please enter a valid directory',
                cursor_position=len(document.text)
            )


def prompt_wrapper(questions):
    answers = prompt(questions)
    # Prompt catches KeyboardInterrupt and sends back an empty dictionary
    # We want to catch this exception
    if len(answers) == 0:
        raise KeyboardInterrupt()
    return answers


def type_validator(t, v):
    """Return a boolean indicating whether `v` can be cast to `t(v)` without raising a ValueError."""
    try:
        t(v)
        return True
    except ValueError:
        return False


def ask(function_name: str,
        msg: str,
        validate_type=None,
        output_transformer: Callable = None,
        **kwargs) -> Union[str, List[str]]:
    """
    Wrap questionary functions to catch escapes and exit gracefully.
    function_name: https://questionary.readthedocs.io/en/stable/pages/types.html#
    """

    # Get the questionary function
    questionary_f = questionary.__dict__.get(function_name)

    # Make sure that the function exists
    assert questionary_f is not None, f"No such questionary function: {function_name}"

    if kwargs.get("use_shortcuts") is None and function_name == "select":
        kwargs["use_shortcuts"] = True

    if validate_type is not None:
        kwargs["validate"] = lambda v: type_validator(validate_type, v)

    # The default value must be a string
    if kwargs.get("default") is not None:
        kwargs["default"] = str(kwargs["default"])

    if kwargs.get("required"):
        del kwargs["required"]
        kwargs["validate"] = lambda val: len(val.strip()) > 0 or "This field is required"

    # Add a spacer line before asking the question
    print("")

    # Get the response
    resp = questionary_f(msg, **kwargs).ask()

    # If the user escaped the question
    if resp is None:
        raise KeyboardInterrupt()

    # If an output transformation function was defined
    if output_transformer is not None:
        # Call the function
        resp = output_transformer(resp)

    # Otherwise
    return resp


def ask_yes_no(msg):
    return ask("select", msg, choices=["Yes", "No"]) == "Yes"


T = TypeVar('T')


def get_id_from_name(items: List[T], name_or_id: str) -> Optional[str]:
    matched = get_item_from_name_or_id(items, name_or_id)
    if not matched:
        item_type = type(items[0]).__name__
        item_names = ", ".join([i.id for i in items])
        raise InputError(f"Could not find {item_type} {name_or_id} - options: {item_names}")
    return matched.id


def get_item_from_name_or_id(items: List[T], name_or_id: str) -> Optional[T]:
    matched = next((p for p in items if p.id == name_or_id), None)
    if matched:
        return matched
    return next((p for p in items if p.name == name_or_id), None)


def validate_files(all_files: List[str], files: List[str], directory: str):
    for file in files:
        if file not in all_files:
            raise InputError(f"File '{file}' not found in directory '{directory}'")
