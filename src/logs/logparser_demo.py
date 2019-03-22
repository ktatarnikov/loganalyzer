
import sys
from Spell import Spell
from Drain import Drain
from preprocessor import Preprocessor
from config import log_params_config
from logparser import LogParser

output_dir = './result/'  # The output directory of parsing results

# TODO
# - parse logs with spaces in the component <:[]>
# - Smart things:
# - 1) Unique exceptions per time window
# - 2) Amount of errors per time window
# - 3) Windows without logging - periods of silence
# - 4) Top Correlated Errors (time windows -> appearance in window -> correlation based on count in windows)
# - 5) General KL-Divergence vs Error KL-divergence per window
# - 6) Diff against normal logs
# - 7) Unique errors across log files
# - 8) Clustered jobs

logparser = LogParser(root_input_dir, output_dir, log_params_config)
logparser.load_data()
