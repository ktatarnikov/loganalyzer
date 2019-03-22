import re
import itertools
from Spell import Spell
from Drain import Drain

class TextIndex:
    def __init__(self, extractor = "Drain", output_dir = "./results"):
        self.texts = dict()
        self.output_dir = output_dir
        self.extractor = self.newExtractor(extractor)
        self.exception_file = open('result/exceptions.txt', "a+")
        self.counter = 0

    def newExtractor(self, extractor):
        if extractor == "Drain":
            st         = 0.5  # Similarity threshold
            depth      = 4  # Depth of all leaf nodes
            return Drain.LogParser(outdir = self.output_dir, depth=depth, st=st)
        elif extractor == "Spell":
            tau = 0.3
            return Spell.LogParser(outdir = self.output_dir, tau=tau)
        else:
            raise ParserError(f"Unknown extractor type {self.extractor}")

    def add(self, log_line):
        self.counter += 1
        text_lines = TextLines(log_line.exception)
        status = self.extractor.add_log(self.counter, text_lines.identities_seq(), "ERROR")
        if status == "existing":
            return text_lines.identities_str()
        self.texts[id] = text_lines
        # TODO index identities
        self.exception_file.write("--------\n")
        self.exception_file.write("id:")
        self.exception_file.write(text_lines.identities_str())
        self.exception_file.write("\n")
        for trace_line in text_lines.lines:
            self.exception_file.write("  " + trace_line)
        return id

    def close(self):
        # self.extractor.printTree(self.extractor.rootNode, 0)
        self.exception_file.close()

class TextLines:
    def __init__(self, lines):
        self.lines = lines
        self.exception_identity_patterns = [\
            "(java|scala|org)[\.a-zA-Z0-9_:$@]*Exception: [\.a-zA-Z0-9_:$@\s]*",\
            "Caused by\: [\.a-zA-Z0-9_$@]*(\:[\.a-zA-Z0-9_$@\=\[\],<>\s]*)?",\
            "Caused by\: [\.a-zA-Z0-9_$@]*\: [\.a-zA-Z0-9_$@\=\[\],<>\s\:]+.*"\
        ]
        self.identities = self.extract_identities(self.exception_identity_patterns, self.lines)

    def _split(self, line):
        return [sp for sp in re.split(r'[\W_]', line) if sp != ""]

    def identities_str(self):
        return "[" + ",".join(self.identities_seq()) + "]"

    def identities_seq(self):
        return list(itertools.chain.from_iterable(self.identities))

    def extract_identities(self, patterns, lines):
        regexps = [re.compile(f'^{p}$') for p in patterns]
        result = []
        for line in lines:
            line = line.strip()
            for regexp in regexps:
                if regexp.search(line):
                    result.append(self._split(line))
        if len(result) == 0:
            result.append(self._split(lines[0]))
        return result

    def equals(self, other):
        if len(other.lines) != len(self.lines):
            return False
        for i in range(0, len(self.lines)):
            if other.lines[i].strip() != self.lines[i].strip():
                return False
        return True
