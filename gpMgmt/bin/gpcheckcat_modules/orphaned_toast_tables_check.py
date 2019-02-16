#!/usr/bin/env python

try:
    from collections import defaultdict, namedtuple
    from gppylib.db import dbconn
    from gpcheckcat_modules.orphan_toast_table_issues import OrphanToastTableIssue, DoubleOrphanToastTableIssue, ReferenceOrphanToastTableIssue, DependencyOrphanToastTableIssue, MismatchOrphanToastTableIssue

except ImportError, e:
    sys.exit('Error: unable to import module: ' + str(e))


OrphanedTable = namedtuple('OrphanedTable', 'oid catname')


class OrphanedToastTablesCheck:
    def __init__(self):
        self.table_to_issue = defaultdict()  # a dictionary of orphan table to issue (header, description, cause)
        self.table_to_segments = defaultdict(list)  # a dictionary of orphan table to affected segments (content_id's)

        self.issues = []
        self.orphaned_tables = set()

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
    dependent_table_name,
    double_orphan_parent_oid,
    double_orphan_parent_name,
    double_orphan_parent_reltoastrelid,
    double_orphan_parent_toast_oid,
    double_orphan_parent_toast_name
FROM (
    SELECT
        tst.gp_segment_id,
        tst.oid AS toast_table_oid,
        tst.relname AS toast_table_name,
        tbl.oid AS expected_table_oid,
        tbl.relname AS expected_table_name,
        dep.refobjid AS dependent_table_oid,
        dep.refobjid::regclass::text AS dependent_table_name,
        dbl.oid AS double_orphan_parent_oid,
        dbl.relname AS double_orphan_parent_name,
        dbl.reltoastrelid AS double_orphan_parent_reltoastrelid,
        dbl_tst.oid AS double_orphan_parent_toast_oid,
        dbl_tst.relname AS double_orphan_parent_toast_name
    FROM
        pg_class tst
        LEFT JOIN pg_depend dep ON tst.oid = dep.objid
        LEFT JOIN pg_class tbl ON tst.oid = tbl.reltoastrelid
        LEFT JOIN pg_class dbl
            ON trim('pg_toast.pg_toast_' FROM tst.oid::regclass::text)::int::regclass::oid = dbl.oid
        LEFT JOIN pg_class dbl_tst ON dbl.reltoastrelid = dbl_tst.oid
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
        dep.refobjid::regclass::text AS dependent_table_name,
        dbl.oid AS double_orphan_parent_oid,
        dbl.relname AS double_orphan_parent_name,
        dbl.reltoastrelid AS double_orphan_parent_reltoastrelid,
        dbl.reltoastrelid AS double_orphan_parent_toast_oid,
        dbl_tst.relname AS double_orphan_parent_toast_name
    FROM gp_dist_random('pg_class') tst
        LEFT JOIN gp_dist_random('pg_depend') dep ON tst.oid = dep.objid AND tst.gp_segment_id = dep.gp_segment_id
        LEFT JOIN gp_dist_random('pg_class') tbl ON tst.oid = tbl.reltoastrelid AND tst.gp_segment_id = tbl.gp_segment_id
        LEFT JOIN gp_dist_random('pg_class') dbl
            ON trim('pg_toast.pg_toast_' FROM tst.oid::regclass::text)::int::regclass::oid = dbl.oid 
            AND tst.gp_segment_id = dbl.gp_segment_id
        LEFT JOIN pg_class dbl_tst ON dbl.reltoastrelid = dbl_tst.oid AND tst.gp_segment_id = dbl_tst.gp_segment_id
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
GROUP BY gp_segment_id, toast_table_oid, toast_table_name, expected_table_oid, expected_table_name, dependent_table_oid, dependent_table_name,
    double_orphan_parent_oid,
    double_orphan_parent_name,
    double_orphan_parent_reltoastrelid,
    double_orphan_parent_toast_oid,
    double_orphan_parent_toast_name;
"""

    def runCheck(self, db_connection):
        orphaned_toast_tables = db_connection.query(self.orphaned_toast_tables_query).dictresult()
        if len(orphaned_toast_tables) == 0:
            return True

        for row in orphaned_toast_tables:
            if row['expected_table_oid'] is None and row['dependent_table_oid'] is None:
                orphan_table = OrphanedTable(row['toast_table_oid'], row['toast_table_name'])
                issue = DoubleOrphanToastTableIssue(row)

            elif row['expected_table_oid'] is None:
                orphan_table = OrphanedTable(row['dependent_table_oid'], row['dependent_table_name'])
                issue = ReferenceOrphanToastTableIssue(row)

            elif row['dependent_table_oid'] is None:
                orphan_table = OrphanedTable(row['expected_table_oid'], row['expected_table_name'])
                issue = DependencyOrphanToastTableIssue(row)

            else:
                orphan_table = OrphanedTable(row['dependent_table_oid'], row['dependent_table_name'])
                issue = MismatchOrphanToastTableIssue(row)

            self.issues.append(issue)
            self.orphaned_tables.add(orphan_table)
            self.table_to_segments[orphan_table].append(row['content_id'])
            self.table_to_issue[orphan_table] = issue

        return False

    def issue_for_table(self, table):
        return self.table_to_issue[table]

    def segments_for_table(self, table):
        return self.table_to_segments[table]

    def rows_for_issue(self, issue_cls):
        return [ issue.row for issue in self.issues if isinstance(issue, issue_cls) ]

    def get_orphaned_tables(self):
        return self.orphaned_tables

    def get_unique_issues_found(self):
        types = set()
        for issue in self.issues:
            types.add(type(issue))
        return types

    def get_fix_text(self):
        log_output = ['\nORPHAN TOAST TABLE FIXES:',
                      '===================================================================']
        for issue in self.get_unique_issues_found():
            log_output += issue.fix_text
        return '\n'.join(log_output)

    def add_repair_statements(self, segments):
        content_id_to_segment_map = self._get_content_id_to_segment_map(segments)

        for issue in self.issues:
            if issue.repair_script:
                content_id_to_segment_map[issue.row['content_id']]['repair_statements'].append(issue.repair_script)

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
