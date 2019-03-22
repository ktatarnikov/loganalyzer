import sys
import re
from Spell import Spell
from Drain import Drain
from preprocessor import Preprocessor
from os import listdir
from os.path import isfile, join

class ParserError(Exception):
    pass

class LogParser:
    def __init__(self, root_dir, output_dir, config, extractor = "Drain"):
        self.config = config
        self.root_dir = root_dir
        self.output_dir = output_dir
        self.extractor = extractor

    def newExtractor(self):
        if self.extractor == "Drain":
            st         = 0.5  # Similarity threshold
            depth      = 4  # Depth of all leaf nodes
            return Drain.LogParser(outdir = self.output_dir, depth=depth, st=st)
        elif self.extractor == "Spell":
            tau = 0.3
            return Spell.LogParser(outdir = self.output_dir, tau=tau)
        else:
            raise ParserError(f"Unknown extractor type {self.extractor}")


    def load_data(self):
        for path_config in self.config["logs"]:
            logical_name = path_config["name"]
            print(f"Parsing {logical_name}...")
            full_path = join(self.root_dir, path_config["input_dir"])
            logfile_pattern = path_config["logfile_pattern"]
            regex = re.compile(f'^{logfile_pattern}$')
            files = [f for f in listdir(full_path) if isfile(join(full_path, f))]
            preprocessor = Preprocessor(directory = full_path, logformat = path_config["logformat"], rex=[])
            for file in files:
                match = regex.search(file)
                if regex.search(file):
                    full_file_path = join(full_path, file)
                    print(f"Preprocessing {full_file_path}...")
                    preprocessor.load_data(full_file_path)
            df_log = preprocessor.get_log_dataframe()
            extractor = self.newExtractor()
            extractor.parse(df_log)
            extractor.outputResult(df_log, extractor.logCluL, path_config["name"])
