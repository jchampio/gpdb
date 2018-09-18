# $Id: $

# Make sure Python loads the modules of this package via absolute paths.
from os.path import abspath as _abspath
__path__[0] = _abspath(__path__[0])

# Set GPHOME based on the library's installation path, if the user hasn't
# already set it. This relies on gppylib being installed at
#
#     ${GPHOME}/bin/gpyylib/
#
# TODO: get rid of this hack, and get rid of GPHOME in general.
import os as _os
import os.path as _path
_os.environ['GPHOME'] = _path.dirname(_path.dirname(_path.dirname(_abspath(__file__))))

# Set PYTHONPATH to include the installation bin/ directory. Several scripts
# just assume that gppylib is globally available.
#
# TODO: get rid of this hack as well, and make scripts think about where their
# custom dependencies are.
if _os.getenv('PYTHONPATH'):
    _os.environ['PYTHONPATH'] = _os.getenv('PYTHONPATH') + _path.pathsep + _path.dirname(_path.dirname(_abspath(__file__)))
else:
    _os.environ['PYTHONPATH'] = _path.dirname(_path.dirname(_abspath(__file__)))
