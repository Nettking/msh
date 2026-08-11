"""Human authentication for the FCP web application.

This package deliberately has no dependency on Federation identities or keys.
"""

from .extension import init_human_auth

__all__ = ["init_human_auth"]
