import unittest
import mock

from gppylib import gparray
from gppylib.programs.clsSystemState import *

class GpStateTestCase(unittest.TestCase):


    # this test should be removed once we have replication slot information for
    # a primary
    def test_add_replication_info_does_nothing_for_primary(self):
        data = mock.Mock()
        primary = gparray.Segment(1, 'p', 2, 'p', 's', 'u', 'localhost', 'localhost', 25432, '/tmp')
        GpSystemStateProgram._add_replication_info(data, primary, None)
        self.assertFalse(data.called)

    def test_add_replication_info_adds_unknowns_if_primary_is_down(self):
        data = GpStateData()
        primary = gparray.Segment(1, 'p', 2, 'p', 's', 'd', 'localhost', 'localhost', 25432, '/tmp')
        GpSystemStateProgram._add_replication_info(data, primary, None)
