import os
import functools
import inspect
import platform

from davinci.services.outlook import DavinciEmail
from .global_config import SYSTEM
from .utils import _full_stack

def email_on_fail(responsible_party, use=SYSTEM):
    """
    Decorator to inform of code failure. Use this on your 'main'
    function if you have defined one.

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
        from davinci.utils.notify import email_on_fail
        
        # For best results,
        # keep the log decorator
        # above the email decorator.
        @log()
        @email_on_fail(['Matthew.Campbell@kencogroup.com'])
        def main(args):
            ...
        
        if __name__ == '__main__':
            main()

    """
    
    def _wrapper(f):
        if use:
            @functools.wraps(f)
            def _func(*args, **kwargs):
                try:                
                    res = f(*args, **kwargs)
                except Exception as e:
                    frame = inspect.currentframe()
                    outer_frame = inspect.getouterframes(frame)[-1]
                    split_on = "\\" if platform.system() == 'Windows' else "/"
                    filepath = os.path.abspath(outer_frame.filename)
                    if "davinci_prod" in filepath:
                        filepath = "".join(filepath.split('davinci_prod')[-1])[1:]
                    file = outer_frame.filename.split(split_on)[-1]
                    trace = _full_stack()
                    func_name = f.__name__
                    trace = trace.replace('\n', '<br />')
                    trace = '<pre>' + trace + '</pre>'
                    debug_email = DavinciEmail(
                        subject=f"CODE FAILURE: {file}",
                        html_body=f"<h3>{filepath}, <br>{func_name}</h3><br /><p>{trace}</p><br />"
                    )
                    debug_email.send(to=responsible_party)
                    raise Exception
                return res
            return _func
        else:
            return f
    return _wrapper