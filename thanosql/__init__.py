from __future__ import annotations

from thanosql._client import ThanoSQL
from thanosql._version import __version__

__all__ = ["ThanoSQL"]

# Load Ipython Magic
from .magic.magic import ThanosMagic


# In order to actually use these magics, you must register them with a
# running IPython.
def load_ipython_extension(ipython):
    """Load the extension in IPython."""
    ipython.register_magics(ThanosMagic)
