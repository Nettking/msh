"""Human authentication for the FCP web application.

Passwords stay on the Federation creator/leader. Trusted Federation members use
short-lived, device-bound signed assertions for browser sign-in; device identity
and human identity remain separate authorization layers.
"""

from .extension import init_human_auth

__all__ = ["init_human_auth"]
