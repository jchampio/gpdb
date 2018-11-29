#!/usr/bin/env python

try:
    from collections import defaultdict, namedtuple
    from gppylib.db import dbconn

except ImportError, e:
    sys.exit('Error: unable to import module: ' + str(e))


OrphanedTable = namedtuple('OrphanedTable', 'oid catname')


class OrphanToastTableIssue(object):
    DOUBLE = 'double_orphans'
    REFERENCE = 'reference_orphans'
    DEPENDENCY = 'dependency_orphans'
    MISMATCH = 'mismatch_orphans'

    def __init__(self, type, cause=None):
        self.type = type
        self.cause = cause

    def __eq__(self, other):
        if isinstance(other, OrphanToastTableIssue):
            return self.type == other.type
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "OrphanToastTableIssue(%s)" % self.type

    def __hash__(self):
        return hash(self.__repr__())

    @classmethod
    def get_issue_repair_script(cls, type, row):
        issue = OrphanToastTableIssue(type)
        return issue.repair_script(row)

    @property
    def fix_text(self):
        fix_text = []
        if self.type == OrphanToastTableIssue.DOUBLE:
            fix_text.append(self.header)
            fix_text.append('''  To fix, run the generated repair script which attempts to determine the original dependent table's OID from the name of the TOAST table.''')
            fix_text.append('''  If the dependent table has a valid OID and exists, the script updates its pg_class entry with the correct reltoastrelid and adds a pg_depend entry.''')
            fix_text.append('''  If the dependent table doesn't exist, the script deletes the associated TOAST table.''')
            fix_text.append('''  If the dependent table is invalid, the associated TOAST table has been renamed. A manual catalog change is needed.\n''')
        elif self.type == OrphanToastTableIssue.REFERENCE:
            fix_text.append(self.header)
            fix_text.append('''  To fix, run the generated repair script which inserts a pg_depend entry using the correct dependent table OID for refobjid.\n''')
        elif self.type == OrphanToastTableIssue.DEPENDENCY:
            fix_text.append(self.header)
            fix_text.append('''  To fix, run the generated repair script which inserts a pg_depend entry using the correct dependent table OID for refobjid.\n''')
        elif self.type == OrphanToastTableIssue.MISMATCH:
            fix_text.append(self.header)
            fix_text.append('''  A manual catalog change is needed to fix by updating the pg_depend TOAST table entry and setting the refobjid field to the correct dependent table.\n''')
        return fix_text

    @property
    def header(self):
        if self.type == OrphanToastTableIssue.DOUBLE:
            return 'Double Orphan TOAST tables due to a missing reltoastrelid in pg_class and a missing pg_depend entry.'
        elif self.type == OrphanToastTableIssue.REFERENCE:
            return 'Bad Reference Orphaned TOAST tables due to a missing reltoastrelid in pg_class'
        elif self.type == OrphanToastTableIssue.DEPENDENCY:
            return 'Bad Dependency Orphaned TOAST tables due to a missing pg_depend entry'
        elif self.type == OrphanToastTableIssue.MISMATCH:
            return 'Mismatch Orphaned TOAST tables due to reltoastrelid in pg_class pointing to an incorrect TOAST table'

    @property
    def description(self):
        if self.type == OrphanToastTableIssue.DOUBLE:
            return 'Found a "double orphan" orphaned TOAST table caused by missing reltoastrelid and missing pg_depend entry.'
        elif self.type == OrphanToastTableIssue.REFERENCE:
            return 'Found a "bad reference" orphaned TOAST table caused by missing a reltoastrelid in pg_class.'
        elif self.type == OrphanToastTableIssue.DEPENDENCY:
            return 'Found a "bad dependency" orphaned TOAST table caused by missing a pg_depend entry.'
        elif self.type == OrphanToastTableIssue.MISMATCH:
            return 'Found a "mismatched" orphaned TOAST table caused by a reltoastrelid in pg_class pointing to an incorrect TOAST table. A manual catalog change is needed.'

    def repair_script(self, row):
        if self.type == OrphanToastTableIssue.DOUBLE:
            # Given a TOAST table oid, get its name, extract the original table's oid from the name, and cast to oid
            extract_oid_expr = "trim('pg_toast.pg_toast_' from %d::regclass::text)::int::regclass::oid" % row["toast_table_oid"]
            return self.__get_double_orphan_repair_statement(extract_oid_expr, row["toast_table_oid"])
        elif self.type == OrphanToastTableIssue.REFERENCE:
            return "UPDATE \"pg_class\" SET reltoastrelid = %d WHERE oid = %s;" % (
                row["toast_table_oid"], row["dependent_table_oid"])
        elif self.type == OrphanToastTableIssue.DEPENDENCY:
            # 1259 is the reserved oid for pg_class and 'i' means internal dependency; these are safe to hard-code
            return "INSERT INTO pg_depend VALUES (1259, %d, 0, 1259, %d, 0, 'i');" % (
                row["toast_table_oid"], row["expected_table_oid"])

    def __get_double_orphan_repair_statement(self, extract_oid_expr, toast_table_oid):
        # First, attempt to determine the original table's oid from the name of the TOAST table.
        # If it's a valid oid and that table exists, update its pg_class entry and add a pg_depend entry.
        # If it's invalid, the TOAST table has been renamed and there's nothing we can do.
        # If the table doesn't exist, we can safely delete the TOAST table.
        # 1259 is the reserved oid for pg_class and 'i' means internal dependency; these are safe to hard-code
        return """DO $$
DECLARE
parent_table_oid oid := 0;
check_oid oid := 0;
toast_table_name text := '';
BEGIN
BEGIN
SELECT oid FROM pg_class WHERE oid = {0} INTO parent_table_oid;
EXCEPTION WHEN OTHERS THEN
-- Invalid oid; maybe the TOAST table was renamed.  Do nothing.
RETURN;
END;

SELECT count(oid) FROM pg_class WHERE oid = parent_table_oid INTO check_oid;
SELECT {1}::regclass::text INTO toast_table_name;
IF check_oid = 0 THEN
-- Parent table doesn't exist.  Drop TOAST table.
DROP TABLE toast_table_name;
RETURN;
END IF;

-- Parent table exists and is valid; go ahead with UPDATE and INSERT
UPDATE pg_class SET reltoastrelid = {1} WHERE oid = parent_table_oid;
INSERT INTO pg_depend VALUES (1259, {1}, 0, 1259, parent_table_oid, 0, 'i');
END
$$;""".format(extract_oid_expr, toast_table_oid)


class OrphanedToastTablesCheck:

    def __init__(self):
        self.table_to_issue = defaultdict()  # a dictionary of orphan table to issue (type, header, description, cause)
        self.table_to_segments = defaultdict(list)  # a dictionary of orphan table to affected segments (content_id's)
        self.issue_to_rows = defaultdict(list)  # a dictionary of issue to individual orphan toast tables

        # Normally, there's a "loop" between a table and its TOAST table:
        # - The table's reltoastrelid field in pg_class points to its TOAST table
        # - The TOAST table has an entry in pg_depend pointing to its table
        # This can break and orphan a TOAST table in one of three ways:
        # - The reltoastrelid entry is set to 0
        # - The reltoastrelid entry is set to a different oid value
        # - The pg_depend entry is missing
        # - The reltoastrelid entry is wrong *and* the pg_depend entry is missing
        # The following query attempts to "follow" the loop from pg_class to
        # pg_depend back to pg_class, and if the table oids don't match and/or
        # one is missing, the TOAST table is considered to be an orphan.
        self.orphaned_toast_tables_query = """
SELECT
    gp_segment_id AS content_id,
    toast_table_oid,
    toast_table_name,
    expected_table_oid,
    expected_table_name,
    dependent_table_oid,
    dependent_table_name
FROM (
    SELECT
        tst.gp_segment_id,
        tst.oid AS toast_table_oid,
        tst.relname AS toast_table_name,
        tbl.oid AS expected_table_oid,
        tbl.relname AS expected_table_name,
        dep.refobjid AS dependent_table_oid,
        dep.refobjid::regclass::text AS dependent_table_name
    FROM
        pg_class tst
        LEFT JOIN pg_depend dep ON tst.oid = dep.objid
        LEFT JOIN pg_class tbl ON tst.oid = tbl.reltoastrelid
    WHERE tst.relkind='t'
        AND	(
            tbl.oid IS NULL
            OR refobjid IS NULL
            OR tbl.oid != dep.refobjid
        )
        AND (
            tbl.relnamespace IS NULL
            OR tbl.relnamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog')
        )
    UNION ALL
    SELECT
        tst.gp_segment_id,
        tst.oid AS toast_table_oid,
        tst.relname AS toast_table_name,
        tbl.oid AS expected_table_oid,
        tbl.relname AS expected_table_name,
        dep.refobjid AS dependent_table_oid,
        dep.refobjid::regclass::text AS dependent_table_name
    FROM gp_dist_random('pg_class') tst
        LEFT JOIN gp_dist_random('pg_depend') dep ON tst.oid = dep.objid AND tst.gp_segment_id = dep.gp_segment_id
        LEFT JOIN gp_dist_random('pg_class') tbl ON tst.oid = tbl.reltoastrelid AND tst.gp_segment_id = tbl.gp_segment_id
    WHERE tst.relkind='t'
        AND (
            tbl.oid IS NULL
            OR refobjid IS NULL
            OR tbl.oid != dep.refobjid
        )
        AND (
            tbl.relnamespace IS NULL
            OR tbl.relnamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog')
        )
    ORDER BY toast_table_oid, expected_table_oid, dependent_table_oid, gp_segment_id
) AS subquery
GROUP BY gp_segment_id, toast_table_oid, toast_table_name, expected_table_oid, expected_table_name, dependent_table_oid, dependent_table_name;
"""

    def runCheck(self, db_connection):
        orphaned_toast_tables = db_connection.query(self.orphaned_toast_tables_query).dictresult()
        if len(orphaned_toast_tables) == 0:
            return True

        for row in orphaned_toast_tables:
            if row['expected_table_oid'] is None and row['dependent_table_oid'] is None:
                orphan_table = OrphanedTable(row['toast_table_oid'], row['toast_table_name'])
                issue = OrphanToastTableIssue(OrphanToastTableIssue.DOUBLE,
                                              '''is an orphan TOAST table.''')

                self.table_to_issue[orphan_table] = issue
                self.table_to_segments[orphan_table].append(row['content_id'])
                self.issue_to_rows[OrphanToastTableIssue(issue.DOUBLE)].append(row)
            elif row['expected_table_oid'] is None:
                orphan_table = OrphanedTable(row['dependent_table_oid'], row['dependent_table_name'])
                issue = OrphanToastTableIssue(OrphanToastTableIssue.REFERENCE,
                                              '''has an orphaned TOAST table '%s' (OID: %s).''' % (row['toast_table_name'], row['toast_table_oid']))

                self.table_to_issue[orphan_table] = issue
                self.table_to_segments[orphan_table].append(row['content_id'])
                self.issue_to_rows[OrphanToastTableIssue(issue.REFERENCE)].append(row)
            elif row['dependent_table_oid'] is None:
                orphan_table = OrphanedTable(row['expected_table_oid'], row['expected_table_name'])
                issue = OrphanToastTableIssue(OrphanToastTableIssue.DEPENDENCY,
                                              '''has an orphaned TOAST table '%s' (OID: %s).''' % (row['toast_table_name'], row['toast_table_oid']))

                self.table_to_issue[orphan_table] = issue
                self.table_to_segments[orphan_table].append(row['content_id'])
                self.issue_to_rows[OrphanToastTableIssue(issue.DEPENDENCY)].append(row)
            else:
                orphan_table = OrphanedTable(row['dependent_table_oid'], row['dependent_table_name'])
                issue = OrphanToastTableIssue(OrphanToastTableIssue.MISMATCH,
                                              '''has an orphaned TOAST table '%s' (OID: %s). Expected dependent table to be '%s' (OID: %s).''' % (row['toast_table_name'], row['toast_table_oid'], row['expected_table_name'], row['expected_table_oid']))

                self.table_to_issue[orphan_table] = issue
                self.table_to_segments[orphan_table].append(row['content_id'])
                self.issue_to_rows[OrphanToastTableIssue(issue.MISMATCH)].append(row)

        return False

    def get_table_to_issue(self):
        return self.table_to_issue

    def get_table_to_segments(self):
        return self.table_to_segments

    def get_issue_to_rows(self):
        return self.issue_to_rows

    def get_orphaned_tables(self):
        return self.table_to_issue.keys()

    def get_unique_issues_found(self):
        return self.issue_to_rows.keys()

    def get_fix_text(self):
        log_output = ['\nORPHAN TOAST TABLE FIXES:',
                      '===================================================================']
        for issue in self.get_unique_issues_found():
            log_output += issue.fix_text
        return '\n'.join(log_output)

    def add_repair_statements(self, segments):
        content_id_to_segment_map = self._get_content_id_to_segment_map(segments)

        for issue in self.get_unique_issues_found():
            for row in self.get_issue_to_rows()[issue]:
                repair_statement = issue.repair_script(row)
                if repair_statement is not None:
                    content_id_to_segment_map[row['content_id']]['repair_statements'].append(repair_statement)

        segments_with_repair_statements = filter(lambda segment: len(segment['repair_statements']) > 0, content_id_to_segment_map.values())
        for segment in segments_with_repair_statements:
            segment['repair_statements'] = ["SET allow_system_table_mods=true;"] + segment['repair_statements']

        return segments_with_repair_statements

    @staticmethod
    def _get_content_id_to_segment_map(segments):
        content_id_to_segment = {}
        for segment in segments.values():
            segment['repair_statements'] = []
            content_id_to_segment[segment['content']] = segment

        return content_id_to_segment
