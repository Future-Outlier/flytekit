"""
.. currentmodule:: flytekitplugins.flyin

This package contains flyin plugin for Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   vscode
   VscodeConfig
   DEFAULT_CODE_SERVER_DIR_NAME
   DEFAULT_CODE_SERVER_REMOTE_PATH
   DEFAULT_CODE_SERVER_EXTENSIONS
   COPILOT_EXTENSION
   VIM_EXTENSION
   CODE_TOGETHER_EXTENSION
   VIM_CONFIG
   COPILOT_CONFIG
   CODE_TOGETHER_CONFIG
   jupyter
"""

from .jupyter_lib.decorator import jupyter
from .vscode_lib.config import (
    CODE_TOGETHER_CONFIG,
    CODE_TOGETHER_EXTENSION,
    COPILOT_CONFIG,
    COPILOT_EXTENSION,
    VIM_CONFIG,
    VIM_EXTENSION,
    VscodeConfig,
)
from .vscode_lib.constants import (
    DEFAULT_CODE_SERVER_DIR_NAME,
    DEFAULT_CODE_SERVER_EXTENSIONS,
    DEFAULT_CODE_SERVER_REMOTE_PATH,
)
from .vscode_lib.decorator import vscode
