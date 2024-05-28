"""Utilities include common patterns that show up across
many projects and are not third-party related. This includes
things like logging configuration, IO, and other package-internal
handling."""

from .logging import log, logger
from .notify import email_on_fail
from .df_engines import _get_engine, _get_read_func, _get_save_func, _VALID_ENGINE_TYPE, _ENGINE, _READER