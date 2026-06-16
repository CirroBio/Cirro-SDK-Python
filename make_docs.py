from pathlib import Path

import pdoc.doc
import pdoc.render

MODULES = [
    "cirro",
    "cirro.sdk",
    "cirro.auth",
    "cirro.services",
    "cirro.models",
    "cirro.config",
    "cirro.helpers.preprocess_dataset",
    "cirro_api_client",
    "cirro_api_client.cirro_client",
    "cirro_api_client.cirro_auth",
    "cirro_api_client.v1.api",
    "cirro_api_client.v1.client",
    "cirro_api_client.v1.models",
]


def _format_module_text(module_name: str) -> str:
    """Render a module's public API as plain text."""
    mod = pdoc.doc.Module.from_name(module_name)
    lines = [f"# {mod.fullname}", ""]
    if mod.docstring:
        lines += [mod.docstring.strip(), ""]

    for member in mod.members.values():
        if member.name.startswith("_"):
            continue
        _format_member(member, lines, depth=2)

    return "\n".join(lines)


def _format_member(member, lines: list, depth: int):
    heading = "#" * depth
    if isinstance(member, pdoc.doc.Class):
        lines.append(f"{heading} class {member.name}")
        if member.docstring:
            lines += ["", member.docstring.strip()]
        lines.append("")
        for child in member.members.values():
            if child.name.startswith("_") and child.name != "__init__":
                continue
            _format_member(child, lines, depth + 1)
    elif isinstance(member, pdoc.doc.Function):
        sig = str(member.signature) if member.signature else "()"
        lines.append(f"{heading} {member.name}{sig}")
        if member.docstring:
            lines += ["", member.docstring.strip()]
        lines.append("")
    elif isinstance(member, pdoc.doc.Variable):
        annotation = f": {member.annotation}" if member.annotation else ""
        lines.append(f"{heading} {member.name}{annotation}")
        if member.docstring:
            lines += ["", member.docstring.strip()]
        lines.append("")


if __name__ == "__main__":
    pdoc.render.configure(
        docformat="google",
        logo="https://static.cirro.bio/Cirro_Logo_Horizontal_Navy.png",
        logo_link="https://cirro.bio",
    )

    pdoc.pdoc(*MODULES, output_directory=Path("./docs/"))

    # Redirect index.html to cirro.html since we want to expose that module first
    with Path('./docs/index.html').open('w') as index:
        index.write('<meta http-equiv="refresh" content="0; URL=cirro.html" />')

    # Generate llms.txt
    sections = [_format_module_text(m) for m in MODULES]
    with Path('./docs/llms.txt').open('w') as llms:
        llms.write("\n\n---\n\n".join(sections))
