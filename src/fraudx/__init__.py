"""FraudX - real-time fraud detection on a polyglot NoSQL platform."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("fraudx")
except PackageNotFoundError:  # running from a source checkout without install
    __version__ = "0.0.0"
