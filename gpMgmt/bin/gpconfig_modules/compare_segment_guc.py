from gpconfig_modules.database_segment_guc import DatabaseSegmentGuc
from gpconfig_modules.file_segment_guc import FileSegmentGuc
from gpconfig_modules.segment_guc import SegmentGuc


class MultiValueGuc(SegmentGuc):
    """
    encapsulate various GUC locations within a given segment.
    A segment can include 2 databases: the primary and a mirror.
    The database value is singular, since we strongly expect the values to be the same, given mirroring.
    However, the file values of primary and mirror can be different. 
    So we model this MultiValueGuc object to accept 2 file values, and one database value.
    """
    def __init__(self, guc1, guc2):
        """
        accept 2 gucs in any order. gucs can be any combination of:

        * database guc
        * file guc
            - primary
            - mirror
        * existing comparison guc, with or without mirror
        """
        self.primary_file_seg_guc = None
        self.mirror_file_seg_guc = None
        self.db_seg_guc = None

        if not guc1 or not guc2:
            raise Exception("comparison requires two gucs")

        SegmentGuc.__init__(self, [guc1.context, guc1.name])
        if guc1.context != guc2.context:
            raise Exception("Not the same context")

        if isinstance(guc1, MultiValueGuc):
            # copy constructor
            self.db_seg_guc = guc1.db_seg_guc
            self.primary_file_seg_guc = guc1.primary_file_seg_guc
            self.mirror_file_seg_guc = guc1.mirror_file_seg_guc

        if isinstance(guc2, MultiValueGuc):
            # copy constructor
            self.db_seg_guc = guc2.db_seg_guc
            self.primary_file_seg_guc = guc2.primary_file_seg_guc
            self.mirror_file_seg_guc = guc2.mirror_file_seg

        if isinstance(guc1, FileSegmentGuc):
            if self.primary_file_seg_guc:
                if self.primary_file_seg_guc.dbid == guc1.dbid:
                    self.primary_file_seg_guc = guc1
                else:
                    self.mirror_file_seg_guc = guc1
            else:
                self.primary_file_seg_guc = guc1

        if isinstance(guc2, FileSegmentGuc):
            if self.primary_file_seg_guc:
                if self.primary_file_seg_guc.dbid == guc2.dbid:
                    self.primary_file_seg_guc = guc2
                else:
                    self.mirror_file_seg_guc = guc2
            else:
                self.primary_file_seg_guc = guc2

        if isinstance(guc1, DatabaseSegmentGuc):
            self.db_seg_guc = guc1

        if isinstance(guc2, DatabaseSegmentGuc):
            self.db_seg_guc = guc2

    def report_success_format(self):
        file_val = self.primary_file_seg_guc.get_value()
        if file_val is not None:
            if self.db_seg_guc:
                result = "%s value: %s | file: %s" % (self.get_label(), self.db_seg_guc.value, file_val)
            else:
                result = "%s value: %s" % (self.get_label(), file_val)
        else:
            if self.db_seg_guc:
                result = "%s value: %s | not set in file" % (self.get_label(), self.db_seg_guc.value)
            else:
                result = "No value is set on %s" % ("master" if self.get_label() == "Master " else "segments")
        return result

    def report_fail_format(self):
        sort_seg_guc_objs = [obj for obj in [self.primary_file_seg_guc, self.mirror_file_seg_guc] if obj]
        sort_seg_guc_objs.sort(key=lambda x: x.dbid)

        if self.db_seg_guc:
            report = [self._report_fail_format_with_database_and_file_gucs(seg_guc_obj) for seg_guc_obj in sort_seg_guc_objs]
        else:
            report = [seg_guc_obj.report_fail_format()[0] for seg_guc_obj in sort_seg_guc_objs]
        return report

    def _report_fail_format_with_database_and_file_gucs(self, segment_guc_obj):
        if segment_guc_obj.value is None:
            file_tag = "not set in file"
        else:
            file_tag = "file: %s" % segment_guc_obj.value

        return "[context: %s] [dbid: %s] [name: %s] [value: %s | %s]" % (
            self.db_seg_guc.context,
            segment_guc_obj.dbid,
            self.db_seg_guc.name,
            self.db_seg_guc.value,
            file_tag)

    class ParseError(Exception):
        pass

    class _StringStream(object):
        """
        A helper class for _unquote() that implements next() and peek()
        operations for a byte string, to turn it into a "stream".
        """
        def __init__(self, s):
            self._str = s
            self._len = len(s)
            self._i = 0

        def peek(self):
            if self._i >= self._len:
                return ''

            return self._str[self._i]

        def next(self):
            if self._i >= self._len:
                return ''

            char = self._str[self._i]
            self._i += 1
            return char

    @staticmethod
    def _isoctal(char):
        return char in ('0', '1', '2', '3', '4', '5', '6', '7')

    @staticmethod
    def _unquote(guc_value):
        if not guc_value:
            raise MultiValueGuc.ParseError('parameter value is empty')

        # Don't unquote values that aren't quoted.
        if guc_value[0] != "'":
            return guc_value

        # Make sure we have an ending quote, then strip them.
        if len(guc_value) == 1 or guc_value[-1] != "'":
            raise MultiValueGuc.ParseError('missing final single quote')

        guc_value = guc_value[1:-1]

        quoted = []
        stream = MultiValueGuc._StringStream(guc_value)
        while True:
            char = stream.next()
            if not char:
                break

            if char == '\\':
                char = stream.next()
                if not char:
                    raise MultiValueGuc.ParseError('invalid trailing backslash')

                # Handle standard backslash escapes.
                if char == 'b':
                    char = '\b'
                elif char == 'f':
                    char = '\f'
                elif char == 'n':
                    char = '\n'
                elif char == 'r':
                    char = '\r'
                elif char == 't':
                    char = '\t'

                # Handle octal escapes (e.g. \023).
                elif MultiValueGuc._isoctal(char):
                    octal = int(char)

                    # Octal escapes can have a maximum of three digits.
                    for _ in range(2):
                        char = stream.peek()
                        if not MultiValueGuc._isoctal(char):
                            break

                        octal = (octal << 3) + int(char)
                        stream.next() # advance

                    # Translate back to a character (truncating to one byte).
                    char = chr(octal & 0xFF)

            # Handle escaped single quotes.
            elif char == "'":
                char = stream.next()
                if char != "'":
                    raise MultiValueGuc.ParseError('invalid single quote')

            quoted.append(char)

        return ''.join(quoted)

    def compare_db_and_file_values(self, db_guc, file_guc):
        db_guc_escaped = db_guc.replace("\b", "\\b") \
                               .replace("\f", "\\f") \
                               .replace("\n", "\\n") \
                               .replace("\r", "\\r") \
                               .replace("\t", "\\t")
        db_guc_list = list(db_guc_escaped)
        db_guc_length = len(db_guc_list)
        db_guc_parsed = []
        for i in range(len(db_guc_list)):
            character = db_guc_list[i]
            if character == '\\':
                k = 0
                next_character = db_guc_list[i + k]
                octal_value = 0
                while k < 3 and (k + i < db_guc_length) and next_character in ['0', '1', '2', '3', '4', '5', '6', '7']:
                    octal_value = (octal_value << 3) + int(db_guc_list[i + k])
                    k += 1
                    next_character = db_guc_list[k]

                i += k - 1
                db_guc_parsed.append(chr(octal_value))
            else:
                db_guc_parsed.append(character)

        if len(file_guc) >= 2 and file_guc[0] == "'" and file_guc[-1] == "'":
            db_guc_parsed.insert(0, "'")
            db_guc_parsed.append("'")

        return db_guc_list == file_guc

    def is_internally_consistent(self):
        if not self.db_seg_guc:
            return self.compare_primary_and_mirror_files()

        if self.primary_file_seg_guc is None:
            return True
        if self.primary_file_seg_guc.get_value() is None:
            return True

        result = True

        if self.mirror_file_seg_guc and self.db_seg_guc:
            result = self.compare_db_and_file_values(self.db_seg_guc.value, self.mirror_file_seg_guc.value)
        if not result:
            return result

        return self.compare_db_and_file_values(self.db_seg_guc.value, self.primary_file_seg_guc.value) and result

    def get_value(self):
        file_value = ""
        if self.primary_file_seg_guc:
            file_value = str(self.primary_file_seg_guc.get_value())
        db_value = ""
        if self.db_seg_guc:
            db_value = str(self.db_seg_guc.get_value())

        return db_value + "||" + file_value

    def set_mirror_file_segment(self, mirror_file_seg):
        self.mirror_file_seg_guc = mirror_file_seg

    def get_primary_dbid(self):
        return self.primary_file_seg_guc.dbid

    def set_primary_file_segment(self, guc):
        self.primary_file_seg_guc = guc

    def compare_primary_and_mirror_files(self):
        if self.primary_file_seg_guc and self.mirror_file_seg_guc:
            return self.primary_file_seg_guc.get_value() == self.mirror_file_seg_guc.get_value()
        return True
