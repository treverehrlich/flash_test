import logging
import functools
import inspect
import platform
import time
import datetime
from typing import Tuple

from pandas import Series, DataFrame
from numpy import ndarray

from .utils import _full_stack
from .global_config import PROD

# creating logger instance
logger = logging.getLogger(__name__)
"""
When necessary, you can export the base logger directly.
This allows for custom inline messages needed beyond
what @log() will capture.

Example usage:

.. code-block:: python

    from davinci.utils.logging import logger
    
    logger.info("my message")

"""

_FORMAT = "[ %(message)s]"
logging.basicConfig(format=_FORMAT)
logger.setLevel(logging.DEBUG)

_DO_NOT_DISPLAY = {Series, DataFrame, ndarray, Tuple}
_MAX_MSG_SIZE = 400


def _truncate_repr(x):
    typ = type(x)
    if typ in _DO_NOT_DISPLAY:
        if hasattr(x, 'shape'):		
            res = f"***{x.__class__.__name__} Shape: {x.shape} ***"
        else:
            res = f"***{x.__class__.__name__} ***"
    else:
        res = x.__repr__()
    if len(res) > _MAX_MSG_SIZE:
        res = res[:_MAX_MSG_SIZE] + ' ...'
    return res

def log(unveil=False, use=PROD):
    """
    The log decorator to easily log function behaviors
    and runtimes.

    :param unveil: Reveal info about params and return.
        Can potentially reveal secret info, so keep this in mind
        when deciding to use this.
    :type unveil: bool
    :param use: Turn the logger on. Defaults to
        True in linux environment, and False otherwise.
    :type use: bool

    Example usage:

    .. code-block:: python

        from davinci.utils.logging import log
        
        @log(unveil=True)
        def foo(non_secret_x):
            ...
        
        @log()
        def bar(private_x):
            ...

    """
    
    def _wrapper(f):
        @functools.wraps(f)
        def _func(*args, **kwargs):
            if use:
                start = time.time()
                start_datetime = datetime.datetime.now().strftime('%m-%d-%Y, @ %H:%M:%S')
                try:
                    frame = inspect.currentframe()
                    try:
                        outer_frame = inspect.getouterframes(frame)[1]
                    except IndexError as e:
                        logger.warn(f'issue getting outer frame info. {e}')
                    split_on = "\\" if platform.system() == 'Windows' else "/"
                    file = outer_frame.filename.split(split_on)[-1]
                    line = outer_frame.lineno
                    track = 'file: {}, line: {}, function: {} -- '.format(file, line, f.__name__)
                    logger.info(track)
                    logger.info(track + 'entering function.')
                    if unveil:
                        params = inspect.getcallargs(f, *args, **kwargs)
                        trunc_params = {param: _truncate_repr(params[param]) for param in params}
                        logger.info(track + f'called with {trunc_params}')
                except Exception as e:
                    logger.info('ERROR IN LOGGING!')
                    logger.info(track + str(e))

            # Make sure the function always executes independent of logging
            try:
                res = f(*args, **kwargs)
            except Exception as e:
                if use:
                    logger.info('ERROR!!! Occurred in ' + track)
                logger.info('Full Stack Trace')	
                logger.info(_full_stack())
                logger.info('Quick Info: ' + str(e))
                raise e
            if use:
                try:
                    if unveil:
                        if hasattr(res, '__iter__') and type(res) != DataFrame:
                            for i in res:
                                res_i = _truncate_repr(i)
                                logger.info(track + f'returned {res_i}')
                        else:
                            logger.info(track + f'returned {_truncate_repr(res)}')
                    if f.__name__.lower() == 'main':
                        logger.info('TOTAL RUNTIME: ' + str(round(time.time() - start, 2)) + 's')
                        end_datetime = datetime.datetime.now().strftime('%m-%d-%Y, @ %H:%M:%S')
                        logger.info(f'Script Start: {start_datetime} >>> Script End: {end_datetime}')
                    else:
                        logger.info(track + 'exitting function, total time ' + str(round(time.time() - start, 2)) + 's')
                except Exception as e:
                    logger.info('ERROR IN LOGGING!')
                    logger.info(track + str(e))
            return res

        return _func
    return _wrapper
