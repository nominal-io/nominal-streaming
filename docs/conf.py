"""Sphinx config for the nominal-streaming docs: the Rust crate and its Python package."""

import re
from pathlib import Path

from nominal_sphinx_theme import theme_options

REPO = Path(__file__).resolve().parent.parent

project = "Streaming"
copyright = "Nominal, Inc."

extensions = [
    "myst_parser",
    "sphinx_design",
    "sphinx_copybutton",
    "sphinx.ext.napoleon",
    "autoapi.extension",
    "nominal_sphinx_theme",
]
myst_enable_extensions = ["attrs_block", "colon_fence"]
exclude_patterns = ["_build"]

# -- Python reference ------------------------------------------------------------------------------
# sphinx-autoapi reads the package's source and its .pyi stub without importing it, so the build
# needs no compiled extension. The stub holds the docs of the classes written in Rust.
autoapi_type = "python"
autoapi_dirs = [str(REPO / "py-nominal-streaming" / "python")]
autoapi_file_patterns = ["*.pyi", "*.py"]
autoapi_root = "ref/python"
autoapi_add_toctree_entry = False
autoapi_keep_files = True
autoapi_member_order = "bysource"
autoapi_options = ["members", "undoc-members", "show-inheritance", "imported-members"]
# The stub imports DataType from nominal_dataset_stream, which imports the stub's classes: fine for
# Python, but autoapi can't follow the cycle. Every class is documented from its own file regardless.
suppress_warnings = ["autoapi.python_import_resolution"]
napoleon_google_docstring = True
napoleon_numpy_docstring = False


# The docstrings put examples in Markdown code fences; Napoleon expects reStructuredText.
CODE_FENCE = re.compile(
    r"^(?P<indent> *)```(?P<language>[\w.+-]*)[ \t]*\n"
    r"(?P<code>(?:(?P=indent)[^\n]*\n|[ \t]*\n)*?)^(?P=indent)```[ \t]*$",
    re.MULTILINE,
)


def expand_code_fence(match: re.Match[str]) -> str:
    body = "\n".join("    " + line if line.strip() else "" for line in match["code"].splitlines())
    return f"{match['indent']}.. code-block:: {match['language'] or 'text'}\n\n{body}\n"


def process_code_fences(app, what, name, obj, options, lines):
    lines[:] = CODE_FENCE.sub(expand_code_fence, "\n".join(lines)).split("\n")


def skip_submodules(app, what, name, obj, skip, options):
    """Document each class once, on the package page: readers import them from nominal_streaming."""
    if what == "module" and name != "nominal_streaming":
        return True
    return skip


def setup(app):
    app.connect("autodoc-process-docstring", process_code_fences, priority=400)  # before Napoleon
    app.connect("autoapi-skip-member", skip_submodules)

html_theme = "shibuya"
# the browser tab and search results; the header shows project
html_title = "nominal-streaming"
html_copy_source = False
html_context = {
    "source_type": "github",
    "source_user": "nominal-io",
    "source_repo": "nominal-streaming",
    "source_version": "main",
    "source_docs_path": "/docs/",
}
html_theme_options = theme_options(
    github_url="https://github.com/nominal-io/nominal-streaming",
    nav_links=[
        {"title": "Quickstart", "url": "index"},
        {"title": "Guides", "url": "guides/index"},
        {"title": "Reference", "url": "ref/index"},
    ],
)
