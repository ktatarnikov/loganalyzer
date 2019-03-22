import re
import os
import sys
import numpy as np
import pandas as pd
import hashlib
from datetime import datetime
from exception import TextIndex, TextLines

class LogLine:
    def __init__(self, line, unclassified = None, exception = []):
        self.logline = line
        self.exception = exception
        self.unclassified = unclassified

    def is_processed(self):
        return self.logline is not None

class LogFSM:
    def __init__(self, line_regex, exception_regexps):
        self.line_regex = line_regex
        self.exception_regexps = [re.compile(f'^{regex}$') for regex in exception_regexps]
        self.non_ascii_regex = r'[^\x00-\x7F]+'

    def process(self, i, lines):
        length = len(lines)
        line = re.sub(self.non_ascii_regex, '<NASCII>', lines[i])
        next_line = re.sub(self.non_ascii_regex, '<NASCII>', lines[i + 1] if i + 1 < length else "")

        is_logline = self._is_log_line(line)
        is_next_logline = self._is_log_line(next_line)
        is_next_exception = self._is_exception_line(next_line)

        logline_followed_by_exception = is_logline and is_next_exception
        logline_followed_by_maybe_exception = is_logline and not is_next_exception and not is_next_logline
        text_followed_by_exception = not is_logline and is_next_exception
        unclassified_lines = not is_logline and not is_next_exception and not is_next_logline
        unclassified_line = not is_logline and is_next_logline

        if logline_followed_by_exception:
            has_exceptions, exception_lines, last_idx = self._scan_maybe_exception(lines, i + 1)
            return last_idx + 1, LogLine(line.strip(), None, exception_lines)
        if logline_followed_by_maybe_exception:
            has_exceptions, exception_lines, last_idx = self._scan_maybe_exception(lines, i + 1)
            if has_exceptions:
                return last_idx + 1, LogLine(line.strip(), None, exception_lines)
            else:
                return i + 1, LogLine(line.strip())
        elif is_logline:
            return i + 1, LogLine(line.strip())
        elif text_followed_by_exception:
            has_exceptions, exception_lines, last_idx = self._scan_maybe_exception(lines, i + 1)
            return last_idx + 1, LogLine(None, line.strip(), exception_lines)
        elif unclassified_lines:
            has_exceptions, exception_lines, last_idx = self._scan_maybe_exception(lines, i + 1)
            return last_idx + 1, LogLine(None, line, exception_lines)
        elif unclassified_line:
            return i + 1, LogLine(None, line, None)
        else:
            print(f"unexpected line: {line}")

        return i + 1, None

    def _is_log_line(self, line):
        return self.line_regex.search(line.strip())

    def _is_exception_line(self, line):
        for regex in self.exception_regexps:
            match = regex.search(line.strip())
            if match:
                return True
        return False

    def _scan_exception(self, lines, current_idx):
        exception_lines = []
        length = len(lines)
        i = current_idx
        while i < length:
            line = lines[i]
            if self._is_exception_line(line):
                exception_lines.append(line)
            else:
                i -= 1
                break
            i += 1
        return exception_lines, i

    def _scan_maybe_exception(self, lines, current_idx):
        exception_lines = []
        length = len(lines)
        i = current_idx
        has_exceptions = False
        while i < length:
            line = lines[i]
            if self._is_exception_line(line):
                has_exceptions |= True
                exception_lines.append(line)
            elif not self._is_log_line(line):
                exception_lines.append(line)
            else:
                i -= 1
                break
            i += 1
        return has_exceptions, exception_lines, i


class JavaExceptionPreprocessor:
    def __init__(self, line_regex):
        exception_match = [
            "[\s]+.*",
            "Caused by\: [\.a-zA-Z0-9_$@]*(\:[\.a-zA-Z0-9_$@\=\[\],<>\s]*)?",
            "Caused by\: [\.a-zA-Z0-9_$@]*\: [\.a-zA-Z0-9_$@\=\[\],<>\s\:]+.*",
            "at [\.a-zA-Z0-9_$@<>]*\([\.a-zA-Z0-9_$@<>\s]*[:[0-9]*]?\).*",
            "\.\.\. [0-9]* more",
            "\.\.\. [0-9]* common frames omitted",
            "(java|scala|org)[\.a-zA-Z0-9_:$@]*Exception: [\.a-zA-Z0-9_:$@\s]*",
            "\]\)",
            "[\.a-zA-Z0-9_$]+\: .*"
        ]
        self.line_regex = line_regex
        self.exception_line_rex = [re.compile(f'^{regex}$') for regex in exception_match]
        self.exception_regex = exception_match

    def preprocess(self, lines):
        fsm = LogFSM(self.line_regex, self.exception_regex)
        result = []
        i = 0
        while i < len(lines):
            i, line = fsm.process(i, lines)
            if line is not None:
                result.append(line)
        return result

class Preprocessor:
    def __init__(self, directory, logformat, rex):
        self.path = directory
        self.logformat = logformat
        self.rex = rex
        self.logdf = None

    def load_data(self, logname):
        file_path = os.path.join(self.path, logname)
        print('Parsing file: ' + file_path)
        lines = []
        with open(file_path, 'r') as fin:
            lines = fin.readlines()
            lines = [line for line in lines if len(line.strip()) > 0]

        headers, regex = self.generate_logformat_regex(self.logformat)
        preprocessed = JavaExceptionPreprocessor(regex).preprocess(lines)
        self.log_to_dataframe(preprocessed, regex, headers, self.logformat)

    def preprocess(self, line):
        for currentRex in self.rex:
            line = re.sub(currentRex, '*', line)
        return line

    def log_to_dataframe(self, lines, regex, headers, logformat):
        """ Function to transform log file to dataframe
        """
        log_messages = []
        linecount = 0
        text_index = TextIndex()
        for line in lines:
            if line.exception is not None and len(line.exception) != 0:
                text_index.add(line)

            if (not line.is_processed()):
                continue

            line = line.logline
            line = re.sub(r'[^\x00-\x7F]+', '<NASCII>', line)
            try:
                match = regex.search(line.strip())
                message = [match.group(header) for header in headers]

                log_messages.append(self.preprocess(message))
                linecount += 1
            except Exception as e:
                pass
        text_index.close()
        current = pd.DataFrame(log_messages, columns=headers)
        current.insert(0, 'LineId', None)

        if self.logdf is None:
            self.logdf = current
        else:
            self.logdf = pd.concat([self.logdf, current])

    def get_log_dataframe(self):
        count = self.logdf.shape[0]
        self.logdf['LineId'] = [i + 1 for i in range(count)]
        return self.logdf

    def generate_logformat_regex(self, logformat):
        """ Function to generate regular expression to split log messages
        """
        headers = []
        splitters = re.split(r'(<[^<>]+>)', logformat)
        regex = ''
        for k in range(len(splitters)):
            if k % 2 == 0:
                splitter = re.sub(' +', '\s+', splitters[k])
                regex += splitter
            else:
                header = splitters[k].strip('<').strip('>')
                regex += '(?P<%s>.*?)' % header
                headers.append(header)
        regex = re.compile('^' + regex + '$')
        return headers, regex
