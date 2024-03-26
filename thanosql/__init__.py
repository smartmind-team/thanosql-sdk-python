from __future__ import annotations

from thanosql._client import ThanoSQL
from thanosql._version import __version__

__all__ = ["ThanoSQL"]


# In order to actually use these magics, you must register them with a
# running IPython.
def load_ipython_extension(ipython):
    # Load Ipython Magic
    from .magic.magic import ThanosMagic
    
    """Load the extension in IPython."""
    ipython.register_magics(ThanosMagic)
