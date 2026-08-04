"""Convert notebook .py (CELL markers) to .ipynb."""

import json
import re
from pathlib import Path

nb_dir = Path(__file__).resolve().parents[1] / "notebooks"

for py in sorted(nb_dir.glob("*.py")):
    text = py.read_text(encoding="utf-8")
    parts = re.split(r"(?m)^# CELL: (markdown|code)\s*$", text)
    cells = []
    preamble = parts[0].strip()
    if preamble.startswith('"""'):
        md = preamble[3:]
        if md.endswith('"""'):
            md = md[:-3]
        md = md.strip()
        cells.append(
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [line + "\n" for line in md.splitlines()],
            }
        )
    i = 1
    while i + 1 < len(parts):
        ctype, body = parts[i], parts[i + 1]
        body = body.strip("\n")
        src_lines = [ln + "\n" for ln in body.splitlines()]
        cell = {
            "cell_type": "markdown" if ctype == "markdown" else "code",
            "metadata": {},
            "source": src_lines,
        }
        if ctype == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
        cells.append(cell)
        i += 2

    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "cells": cells,
    }
    out = py.with_suffix(".ipynb")
    out.write_text(json.dumps(nb, indent=2), encoding="utf-8")
    print(f"Wrote {out.name} ({len(cells)} cells)")
