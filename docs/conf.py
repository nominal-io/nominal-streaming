"""Sphinx config for the nominal-streaming docs: the Rust crate and its Python package."""

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
    # the docstrings put examples in Markdown code fences
    "nominal_sphinx_theme.docstring_fences",
    "sphinx_rustdoc",
]
myst_enable_extensions = ["attrs_block", "colon_fence"]
exclude_patterns = ["_build"]

# -- Rust API reference ----------------------------------------------------------------------------
# sphinx-rustdoc builds the crate's reference under ref/rust/ from rustdoc's JSON output, which
# `cargo rustdoc` writes. The JSON format changes between Rust releases; CI pins the one
# sphinx-rustdoc reads.
rustdoc_crates = {"nominal_streaming": {"package": "nominal-streaming"}}
# docs.rs hasn't built nominal-api's recent releases, so its types render as text
rustdoc_unlinked_crates = ["nominal_api", "nominal_api_conjure", "nominal_api_proto"]

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
        {"title": "Examples", "url": "examples/index"},
        {"title": "Reference", "url": "ref/index"},
    ],
)


def skip_submodules(app, what, name, obj, skip, options):
    """Document each class once, on the package page: readers import them from nominal_streaming."""
    if what == "module" and name != "nominal_streaming":
        return True
    return skip


def setup(app):
    app.connect("autoapi-skip-member", skip_submodules)
