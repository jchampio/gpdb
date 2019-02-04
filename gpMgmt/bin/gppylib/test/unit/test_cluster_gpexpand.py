#! /usr/bin/env python

import imp
import os.path
import unittest
import subprocess

from gppylib import gparray, gplog
from gppylib.db import dbconn

# gpexpand doesn't have an implementation module, so instead of
#     import gpexpand
# we have this quick-and-dirty way of effectively doing the same thing.
_gpexpand_path = os.path.abspath(os.path.dirname(__file__) + "/../../../gpexpand")
gpexpand = imp.load_source('gpexpand', _gpexpand_path)

class GpExpandTestCase(unittest.TestCase):
    test_db = '__test_gpexpand'

    def setUp(self):
        # Create a database to use and some simple tables inside it.
        subprocess.check_call(['createdb', self.test_db])
        subprocess.check_call(['psql', '-c', (
                'CREATE TABLE test (a int, b int) DISTRIBUTED BY (a);'
                'CREATE TABLE test2 (a int, b int) DISTRIBUTED BY (a, b);'
                'CREATE TABLE test3 (a int, b int) DISTRIBUTED RANDOMLY;'
            ), self.test_db])

        # Create a gpexpand object, as if the script were called with no
        # arguments.
        url = dbconn.DbURL(dbname=self.test_db)
        array = gparray.GpArray.initFromCatalog(url)
        options, _ = gpexpand.validate_options(*gpexpand.parseargs(args=[]))

        self.gpexpand = gpexpand.gpexpand(
            gplog.get_unittest_logger(),
            array,
            url,
            options
        )

    def tearDown(self):
        try:
            self.gpexpand.shutdown()
        finally:
            subprocess.check_call(['dropdb', self.test_db])

    def test_stuff(self):
        dbconn.execSQL(self.gpexpand.conn, gpexpand.create_schema_sql)
        dbconn.execSQL(self.gpexpand.conn, gpexpand.status_detail_table_sql)
        self.gpexpand._populate_regular_tables(self.test_db)

if __name__ == '__main__':
    unittest.main()
