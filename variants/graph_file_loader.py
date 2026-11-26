"""Anti-pattern helper: a 'config' module that loads the graph file when the worker process imports it.

Imported with imports_passed_through(), so the sandbox lets it through. A worker restart re-imports it
and therefore reads whatever the file contains at that moment.
"""

import json
import os
from pathlib import Path

GRAPH = json.loads(Path(os.environ["DSL_GRAPH_FILE"]).read_text())
