import subprocess

def _full_stack():
    import traceback, sys
    exc = sys.exc_info()[0]
    stack = traceback.extract_stack()[:-1]  # last one would be _full_stack()
    if exc is not None:  # i.e. an exception is present
        del stack[-1]       # remove call of _full_stack, the printed exception
                            # will contain the caught exception caller instead
    trc = 'Traceback (most recent call last):\n\n'
    stackstr = trc + ''.join(traceback.format_list(stack))
    if exc is not None:
         stackstr += '  ' + traceback.format_exc().lstrip(trc)
    return stackstr

def _parse_doppler_list(s):
    """
    Parses the input string as semi-color delimited
    if any semi-colons exist.
    """
    return [x.strip() for x in s.split(';')] if ';' in s else s
    

def get_git_revision_hash() -> str:
    """
    Returns a string containing the git revision hash value as a string.
    This is useful for keeping track of other integrations such as Prefect
    Deployment versions.
    """
    try:
        return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('ascii').strip()
    except:
        return "Could not find Git Repository"