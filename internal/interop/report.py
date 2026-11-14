"""Require one named result; a successful runner exit alone is insufficient."""
import json
import pathlib
import re
import sys

kind, name, filename, *extra = sys.argv[1:]
output = re.sub(r"\x1b\[[0-9;]*m", "", pathlib.Path(filename).read_text())
if kind.startswith("paho"):
    valid = re.search(r"Ran 1 test in", output)
    passed = re.search(r"^OK$", output, re.M)
elif kind == "mqtt5test":
    context = json.loads(pathlib.Path(extra[0]).read_text())[name]
    rows = re.findall(r"^\s*\[\s*(PASS|FAIL|YES|NO|SKIP)\]\s+(?:MUST|SHOULD|MAY)\s+\[([^]]+)\] (.*)$", output, re.M)
    valid = (len(rows) == 1 and rows[0][1].strip() == ", ".join(context["refs"])
             and (rows[0][2] == context["description"] or rows[0][2].startswith(context["description"] + " —")))
    passed = rows[0][0] in ("PASS", "YES") if rows else False
else:
    rows = re.findall(r"^\s*([✓✗○]) \[(?:PASS|FAIL|SKIP)\] (MQTT-\S+)", output, re.M)
    # Failure details repeat the ID; the summary must still say one test.
    valid = rows and len(rows) == 1 and rows[0][1] == name.split(".", 1)[1] and re.search(r"• 1 total", output)
    passed = rows[0][0] == "✓" if rows else False
if not valid:
    print("missing or ambiguous result for " + name, file=sys.stderr)
    sys.exit(2)
sys.exit(0 if passed else 1)
