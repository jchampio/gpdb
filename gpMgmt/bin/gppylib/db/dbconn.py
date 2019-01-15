#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved.
#
"""
TODO: module docs
"""
import collections
import os
import stat
import sys

try:
    import pgdb
    from gppylib.commands.unix import UserId
except ImportError, e:
    sys.exit('Error: unable to import module: ' + str(e))

from gppylib import gplog

logger = gplog.get_default_logger()


class ConnectionError(StandardError): pass

class Pgpass():
    """ Class for handling .pgpass file.
    """
    entries = []
    valid_pgpass = True

    def __init__(self):
        HOME = os.getenv('HOME')
        PGPASSFILE = os.getenv('PGPASSFILE', '%s/.pgpass' % HOME)

        if not os.path.exists(PGPASSFILE):
            return

        st_info = os.stat(PGPASSFILE)
        mode = str(oct(st_info[stat.ST_MODE] & 0777))

        if mode != "0600":
            print 'WARNING: password file "%s" has group or world access; permissions should be u=rw (0600) or less' % PGPASSFILE
            self.valid_pgpass = False
            return

        try:
            fp = open(PGPASSFILE, 'r')
            try:
                lineno = 1
                for line in fp:
                    line = line.strip()
                    if line.startswith('#'):
                        continue
                    try:
                        (hostname, port, database, username, password) = line.strip().split(':')
                        entry = {'hostname': hostname,
                                 'port': port,
                                 'database': database,
                                 'username': username,
                                 'password': password }
                        self.entries.append(entry)
                    except:
                        print 'Invalid line in .pgpass file.  Line number %d' % lineno
                    lineno += 1
            except IOError:
                pass
            finally:
                if fp: fp.close()
        except OSError:
            pass


    def get_password(self, username, hostname, port, database):
        for entry in self.entries:
            if ((entry['hostname'] == hostname or entry['hostname'] == '*') and
               (entry['port'] == str(port) or entry['port'] == '*') and
               (entry['database'] == database or entry['database'] == '*') and
               (entry['username'] == username or entry['username'] == '*')):
                return entry['password']
        return None

    def pgpass_valid(self):
        return self.valid_pgpass

class DbURL:
    """ DbURL is used to store all of the data required to get at a PG
        or GP database.

    """
    pghost='foo'
    pgport=5432
    pgdb='template1'
    pguser='username'
    pgpass='pass'
    timeout=None
    retries=None

    def __init__(self,hostname=None,port=0,dbname=None,username=None,password=None,timeout=None,retries=None):

        if hostname is None:
            self.pghost = os.environ.get('PGHOST', 'localhost')
        else:
            self.pghost = hostname

        if port is 0:
            self.pgport = int(os.environ.get('PGPORT', '5432'))
        else:
            self.pgport = int(port)

        if dbname is None:
            self.pgdb = os.environ.get('PGDATABASE', 'template1')
        else:
            self.pgdb = dbname

        if username is None:
            self.pguser = os.environ.get('PGUSER', os.environ.get('USER', UserId.local('Get uid')))
            if self.pguser is None or self.pguser == '':
                raise Exception('Both $PGUSER and $USER env variables are not set!')
        else:
            self.pguser = username

        if password is None:
            pgpass = Pgpass()
            if pgpass.pgpass_valid():
                password = pgpass.get_password(self.pguser, self.pghost, self.pgport, self.pgdb)
                if password:
                    self.pgpass = password
                else:
                    self.pgpass = os.environ.get('PGPASSWORD', None)
        else:
            self.pgpass = password

        if timeout is not None:
            self.timeout = int(timeout)

        if retries is None:
            self.retries = 1
        else:
            self.retries = int(retries)


    def __str__(self):

        # MPP-13617
        def canonicalize(s):
            if ':' not in s: return s
            return '[' + s + ']'

        return "%s:%d:%s:%s:%s" % \
            (canonicalize(self.pghost),self.pgport,self.pgdb,self.pguser,self.pgpass)

class ClosingConnection(pgdb.Connection):
    def __init__(self, connection):
        self._notices = collections.deque(maxlen=100)

        def handle_notice(notice):
            self._notices.append(notice)

        self._impl = connection
        self._impl._cnx.set_notice_receiver(handle_notice)

    def __enter__(self):
        return self._impl.__enter__()

    def __exit__(self, *args):
        exit_return = self._impl.__exit__(*args)
        self._impl.close()
        return exit_return

    def __getattr__(self, name):
        return getattr(self._impl, name)

    def notices(self):
        notice_list = list(self._notices)
        self._notices.clear()
        return notice_list

def connect(dburl, utility=False, verbose=False,
            encoding=None, allowSystemTableMods=False, logConn=True):

    conninfo = {
        'user': dburl.pguser,
        'password': dburl.pgpass,
        'host': dburl.pghost,
        'port': int(dburl.pgport),
        'database': dburl.pgdb,
    }

    # building options
    options = ""
    if utility:
        options += '-c gp_session_role=utility '

    # MPP-13779, et al
    if allowSystemTableMods:
        options += '-c allow_system_table_mods=true '

    if options:
        conninfo['options'] = options

    # MPP-14121, use specified connection timeout
    # Single quote the connection string for dbbase name
    retries = 1
    if dburl.timeout is not None:
        conninfo['connection_timeout'] = dburl.timeout
        retries = dburl.retries

    # This flag helps to avoid logging the connection string in some special
    # situations as requested
    if logConn:
        logFunc = logger.info if dburl.timeout is not None else logger.debug
        logFunc("Connecting to db {} on host {}".format(dburl.pgdb, dburl.pghost))

    connection = None
    for i in range(retries):
        try:
            connection = pgdb.connect(**conninfo)
            break

        except pgdb.OperationalError, e:
            if 'timeout expired' in str(e):
                logger.warning('Timeout expired connecting to %s, attempt %d/%d' % (dburl.pgdb, i+1, retries))
                continue
            raise

    if connection is None:
        raise ConnectionError('Failed to connect to %s' % dburl.pgdb)

    #by default, libpq will print WARNINGS to stdout
    if not verbose:
        cursor=connection.cursor()
        cursor.execute("SET CLIENT_MIN_MESSAGES='ERROR'")
        connection.commit()
        cursor.close()

    # set client encoding if needed
    if encoding:
        cursor=connection.cursor()
        cursor.execute("SET CLIENT_ENCODING='%s'" % encoding)
        connection.commit()
        cursor.close()

    return ClosingConnection(connection)

def execSQL(conn,sql):
    """
    If necessary, user must invoke conn.commit().
    Do *NOT* violate that API here without considering
    the existing callers of this function.
    """
    cursor=conn.cursor()
    cursor.execute(sql)
    return cursor

def execSQLForSingletonRow(conn, sql):
    """
    Run SQL that returns exactly one row, and return that one row

    TODO: Handle like gppylib.system.comfigurationImplGpdb.fetchSingleOutputRow().
    In the event of the wrong number of rows/columns, some logging would be helpful...
    """
    cursor=conn.cursor()
    cursor.execute(sql)

    if cursor.rowcount != 1 :
        raise UnexpectedRowsError(1, cursor.rowcount, sql)

    res = cursor.fetchall()[0]
    cursor.close()
    return res

class UnexpectedRowsError(Exception):
    def __init__(self, expected, actual, sql):
        self.expected, self.actual, self.sql = expected, actual, sql
        Exception.__init__(self, "SQL retrieved %d rows but %d was expected:\n%s" % \
                                 (self.actual, self.expected, self.sql))

def execSQLForSingleton(conn, sql):
    """
    Run SQL that returns exactly one row and one column, and return that cell

    TODO: Handle like gppylib.system.comfigurationImplGpdb.fetchSingleOutputRow().
    In the event of the wrong number of rows/columns, some logging would be helpful...
    """
    row = execSQLForSingletonRow(conn, sql)
    if len(row) > 1:
        raise Exception("SQL retrieved %d columns but 1 was expected:\n%s" % \
                         (len(row), sql))
    return row[0]


def executeUpdateOrInsert(conn, sql, expectedRowUpdatesOrInserts):
    cursor=conn.cursor()
    cursor.execute(sql)

    if cursor.rowcount != expectedRowUpdatesOrInserts :
        raise Exception("SQL affected %s rows but %s were expected:\n%s" % \
                        (cursor.rowcount, expectedRowUpdatesOrInserts, sql))
    return cursor
