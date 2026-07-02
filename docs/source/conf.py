# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "cs-geodata"
copyright = "2026, Apoorva Kashyap"
author = "Apoorva Kashyap"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.napoleon",
    "myst_parser",
    "sphinx_copybutton",
    "autoapi.extension",
    "sphinxcontrib.mermaid",
    "sphinx.ext.todo",
]

todo_include_todos = True

mermaid_init_js = """
mermaid.initialize({theme: "default", flowchart: {defaultRenderer: "elk"}});
"""

templates_path = ["_templates"]
exclude_patterns = []

# AutoAPI configuration
autoapi_type = "python"
autoapi_dirs = ["../../src"]
autoapi_ignore = ["*__main__.py", "*_version.py"]

# Optional but recommended
autoapi_keep_files = True
autoapi_add_toctree_entry = True
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
]

# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_use_ivar = True

# MyST markdown support
myst_enable_extensions = [
    "colon_fence",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "shibuya"
html_static_path = ["_static"]
