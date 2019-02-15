#!/usr/bin/env python


class OrphanToastTableIssue(object):
    def __init__(self, cause=None, row=None):
        self.cause = cause
        self.row = row

    def __eq__(self, other):
        if isinstance(other, OrphanToastTableIssue):
            return self.cause == other.cause and self.row == self.row
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "OrphanToastTableIssue(%s,%s)" % (self.cause, self.row)

    def __hash__(self):
        return hash(self.__repr__())

    @property
    def fix_text(self):
        return []

    @property
    def header(self):
        return ''

    @property
    def description(self):
        return ''

    @property
    def repair_script(self):
        return ''


class DoubleOrphanToastTableIssue(OrphanToastTableIssue):
    def __init__(self, row=None):
        if row is None:
            OrphanToastTableIssue.__init__(self, cause=None, row=None)
            return

        cause = ""
        if row['double_orphan_parent_reltoastrelid'] is None:
            cause = '''The parent table does not exist. Therefore, the toast table '%(toast_table_name)s' (oid: %(toast_table_oid)s) can likely be dropped.\n'''
            cause = cause % dict(toast_table_oid=row['toast_table_oid'],
                                 toast_table_name=row['toast_table_name'])
        elif row['double_orphan_parent_oid'] and row['double_orphan_parent_toast_oid']:
            cause = '''with parent table '%(double_orphan_parent_name)s' (oid: %(double_orphan_parent_oid)s). '''
            cause += '''The parent table already references a valid toast table '%(double_orphan_parent_toast_name)s' (oid: %(double_orphan_parent_toast_oid)s). '''
            cause += '''\nTo fix, either:\n  1) drop the orphan toast table '%(toast_table_name)s' (oid: %(toast_table_oid)s)\nor:'''
            cause += '''\n  2a) drop the pg_depend entry for '%(double_orphan_parent_toast_name)s' (oid: %(double_orphan_parent_toast_oid)s)'''
            cause += '''\n  2b) drop the toast table '%(double_orphan_parent_toast_name)s' (oid: %(double_orphan_parent_toast_oid)s),'''
            cause += '''\n  2c) add pg_depend entry where objid is '%(toast_table_name)s' (oid: %(toast_table_oid)s) and refobjid is '%(double_orphan_parent_name)s' (oid: %(double_orphan_parent_oid)s) with deptype 'i'.'''
            cause += '''\n  2d) update the pg_class entry for '%(double_orphan_parent_name)s' (oid: %(double_orphan_parent_oid)s), and set reltoastrelid to '%(toast_table_name)s' (oid: %(toast_table_oid)s).\n'''
            cause = cause % dict(toast_table_oid=row['toast_table_oid'],
                                 toast_table_name=row['toast_table_name'],
                                 double_orphan_parent_oid=row['double_orphan_parent_oid'],
                                 double_orphan_parent_name=row['double_orphan_parent_name'],
                                 double_orphan_parent_toast_oid=row['double_orphan_parent_toast_oid'],
                                 double_orphan_parent_toast_name=row['double_orphan_parent_toast_name'])
        elif row['double_orphan_parent_oid'] and row['double_orphan_parent_toast_oid'] is None:
            cause = '''with parent table '%(double_orphan_parent_name)s' (oid: %(double_orphan_parent_oid)s) has no valid toast table. '''
            cause += '''Verify that the parent table requires a toast table. To fix:'''
            cause += '''\n  1) update the pg_class entry for '%(double_orphan_parent_name)s' (oid: %(double_orphan_parent_oid)s) and set reltoastrelid to '%(toast_table_name)s' (oid: %(toast_table_oid)s).'''
            cause += '''\n  2) add pg_depend entry where objid is '%(toast_table_name)s' (oid: %(toast_table_oid)s) and refobjid is '%(double_orphan_parent_name)s' (oid: %(double_orphan_parent_oid)s) with deptype 'i'.\n'''
            cause = cause % dict(toast_table_oid=row['toast_table_oid'],
                                 toast_table_name=row['toast_table_name'],
                                 double_orphan_parent_oid=row['double_orphan_parent_oid'],
                                 double_orphan_parent_name=row['double_orphan_parent_name'])

        OrphanToastTableIssue.__init__(self, cause, row)

    @property
    def fix_text(self):
        return [self.header,
                '''  A manual catalog change is needed to fix. Attempt to determine the original dependent table's OID from the name of the TOAST table.\n''',
                '''  If the dependent table has a valid OID and exists, the update its pg_class entry with the correct reltoastrelid and adds a pg_depend entry.\n''',
                '''  If the dependent table doesn't exist, delete the associated TOAST table.\n''',
                '''  If the dependent table is invalid, the associated TOAST table has been renamed. A manual catalog change is needed.\n''']

    @property
    def header(self):
        return 'Double Orphan TOAST tables due to a missing reltoastrelid in pg_class and a missing pg_depend entry.'

    @property
    def description(self):
        return 'Found a "double orphan" orphaned TOAST table caused by missing reltoastrelid and missing pg_depend entry.'

    @property
    def repair_script(self):
        # There are multiple ways a double orphan toast table can occur and thus be fixed.
        # We need DBA input to determine how it is broken in order to safely fix it.
        # Since, at this time, we cannot get user input, we chose not to generate a repair script
        # in order to be conservative.
        return ''

class ReferenceOrphanToastTableIssue(OrphanToastTableIssue):
    def __init__(self, row=None):
        if row is None:
            OrphanToastTableIssue.__init__(self, cause=None, row=None)
        else:
            cause = '''has an orphaned TOAST table '%s' (OID: %s).''' % (row['toast_table_name'], row['toast_table_oid'])
            OrphanToastTableIssue.__init__(self, cause, row)

    @property
    def fix_text(self):
        return [self.header,
                '''  To fix, run the generated repair script which updates a pg_class entry using the correct dependent table OID for reltoastrelid.\n''']

    @property
    def header(self):
        return 'Bad Reference Orphaned TOAST tables due to a missing reltoastrelid in pg_class'

    @property
    def description(self):
        return 'Found a "bad reference" orphaned TOAST table caused by missing a reltoastrelid in pg_class.'

    @property
    def repair_script(self):
        return '''UPDATE "pg_class" SET reltoastrelid = %d WHERE oid = %s;''' % (
            self.row["toast_table_oid"], self.row["dependent_table_oid"])


class DependencyOrphanToastTableIssue(OrphanToastTableIssue):
    def __init__(self, row=None):
        if row is None:
            OrphanToastTableIssue.__init__(self, cause=None, row=None)
        else:
            cause = '''has an orphaned TOAST table '%s' (OID: %s).'''% (row['toast_table_name'], row['toast_table_oid'])
            OrphanToastTableIssue.__init__(self, cause, row)

    @property
    def fix_text(self):
        return [self.header,
                '''  To fix, run the generated repair script which inserts a pg_depend entry using the correct dependent table OID for refobjid.\n''']

    @property
    def header(self):
        return 'Bad Dependency Orphaned TOAST tables due to a missing pg_depend entry'

    @property
    def description(self):
        return 'Found a "bad dependency" orphaned TOAST table caused by missing a pg_depend entry.'

    @property
    def repair_script(self):
        # 1259 is the reserved oid for pg_class and 'i' means internal dependency; these are safe to hard-code
        return '''INSERT INTO pg_depend VALUES (1259, %d, 0, 1259, %d, 0, 'i');''' % (
            self.row["toast_table_oid"], self.row["expected_table_oid"])


class MismatchOrphanToastTableIssue(OrphanToastTableIssue):
    def __init__(self, row=None):
        if row is None:
            OrphanToastTableIssue.__init__(self, cause=None, row=None)
        else:
            cause = '''has an orphaned TOAST table '%s' (OID: %s). Expected dependent table to be '%s' (OID: %s).''' % (
                row['toast_table_name'], row['toast_table_oid'], row['expected_table_name'], row['expected_table_oid'])
            OrphanToastTableIssue.__init__(self, cause, row)

    @property
    def fix_text(self):
        return [self.header,
                '''  A manual catalog change is needed to fix by updating the pg_depend TOAST table entry and setting the refobjid field to the correct dependent table.\n''']

    @property
    def header(self):
        return 'Mismatch Orphaned TOAST tables due to reltoastrelid in pg_class pointing to an incorrect TOAST table'

    @property
    def description(self):
        return 'Found a "mismatched" orphaned TOAST table caused by a reltoastrelid in pg_class pointing to an incorrect TOAST table. A manual catalog change is needed.'

    @property
    def repair_script(self):
        # There can be a cyclic reference in reltoastrelid in pg_class which
        # is difficult to determine the valid toast table (if any).
        # Therefore, no repair script can safely be generated and
        # not loose customer data.
        return ''
