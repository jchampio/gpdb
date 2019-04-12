# $Id: $

# Make sure Python loads the modules of this package via absolute paths.
from os.path import abspath as _abspath
__path__[0] = _abspath(__path__[0])

def _sanitize_pythonpath():
    """
    Sanitize the PYTHONPATH to remove the hack introduced by gp-python-selector.
    """
    import os
    import os.path

    pythonpath = os.environ.get('PYTHONPATH')
    if not pythonpath:
        return

    paths = pythonpath.split(os.pathsep)
    hack_path = os.path.expandvars('${GPHOME}/lib/python')

    if paths[0] == hack_path:
        paths = paths[1:]
        # FIXME: if PYTHONPATH was unset to start with, we need to unset it here
        os.environ['PYTHONPATH'] = os.pathsep.join(paths)

_sanitize_pythonpath()
