"""Read-only Polymarket research and analytics toolkit."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("polymarket-insight")
except PackageNotFoundError:  # pragma: no cover - editable tree before install
    __version__ = "0.1.0"

__all__ = ["__version__"]
