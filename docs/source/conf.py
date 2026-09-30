"""Sphinx configuration for the procrastinators documentation."""

from __future__ import annotations

import typing

__author__ = "Rohan B. Dalton"

import importlib.metadata
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sphinx.application import Sphinx
else:
    pass

project = "procrastinators"
author = "Rohan B. Dalton"
release = importlib.metadata.version("procrastinators")
version = release

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
]

source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
templates_path = ["_templates"]
exclude_patterns = list()

autosummary_generate = True
autosummary_imported_members = False
autodoc_default_options = {
    "member-order": "bysource",
    "show-inheritance": True,
}
autodoc_typehints = "description"
autodoc_typehints_description_target = "documented_params"

myst_heading_anchors = 3

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# Optional drivers' client types, named in signatures but imported only for type
# checking, so no inventory can resolve them.
nitpick_ignore = [
    ("py:class", name)
    for name in (
        "AsyncClient",
        "Client",
        "SyncClient",
        "psycopg.AsyncConnection",
        "psycopg.Connection",
    )
]

html_theme = "sphinx_rtd_theme"
html_title = f"{project} {release}"


def _skip_private_bases(
    app: Sphinx, name: str, obj: object, options: dict[str, bool], bases: list[type]
) -> None:
    # Private helper bases such as ``_BackendLifecycle`` have no page of their own; show
    # what they inherit from instead so every rendered base is a working link.
    # A parameterized base such as ``_Handle[Connection]`` is expanded through its origin.
    expanded = list()
    for base in bases:
        origin = typing.get_origin(base) or base
        if origin.__name__.startswith("_"):
            expanded.extend(base for base in origin.__bases__ if base is not typing.Generic)
        else:
            expanded.append(base)
    bases[:] = [base for base in dict.fromkeys(expanded) if base is not object]


def setup(app: Sphinx) -> dict[str, bool]:
    app.connect("autodoc-process-bases", _skip_private_bases)
    metadata = {"parallel_read_safe": True, "parallel_write_safe": True}
    return metadata


if __name__ == "__main__":
    pass
else:
    pass
