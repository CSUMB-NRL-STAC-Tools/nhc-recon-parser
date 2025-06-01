# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
sys.path.insert(0, os.path.abspath('../src'))
project = 'NHC Recon STAC Tools'
copyright = '2025, Zachary Davis'
author = 'Zachary Davis'
version = '1.0'
release = '1.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',       # Core extension for docstring processing
    'sphinx.ext.napoleon',      # For Google-style or NumPy-style docstrings
    'sphinx.ext.autosummary',   # Generates summary tables (useful for modules)
    'sphinx.ext.viewcode',      # Adds links to highlighted source code
    'sphinx.ext.intersphinx',   # Links to documentation of other projects
    'sphinx.ext.todo',          # Allows for todo notes
    'sphinx.ext.coverage',      # Checks for documented members
]

templates_path = ['_templates']

source_suffix = '.rst'
master_doc = 'index'

exclude_patterns = []
pygments_style = 'sphinx'
html_theme = 'furo'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_static_path = ['_static']
autodoc_member_order = 'bysource' # Order members by their appearance in the source file
autodoc_default_options = {
    'members': True,          # Document all public members (functions, classes, methods)
    'undoc-members': True,    # Also document members without docstrings
    'show-inheritance': True, # Show base classes
    'inherited-members': True, # Document inherited members
    'special-members': '__init__', # Document __init__ method
    # 'private-members': True,  # Uncomment if you need to document private members
    # 'no-inherited-members': True, # Uncomment if you DON'T want inherited members documented
}