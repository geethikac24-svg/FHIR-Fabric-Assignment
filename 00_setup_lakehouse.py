"""
# 00 — Lakehouse Setup (Microsoft Fabric)

Attach this notebook to your **FHIR Lakehouse**.

Creates folder layout and registers schemas. Run once per workspace.
"""

# CELL: markdown
# ## Medallion folder layout
# ```
# Files/
#   raw/fhir/{patient|encounter|observation|condition}/YYYY/MM/DD/
#   logs/
# Tables/
#   bronze_* / silver_* / gold_*
# ```

# CELL: code
from pathlib import Path

# Fabric Files mount (adjust if your lakehouse path differs)
FILES_ROOT = Path("/lakehouse/default/Files")
TABLES_HINT = "/lakehouse/default/Tables"

folders = [
    "raw/fhir/patient",
    "raw/fhir/encounter",
    "raw/fhir/observation",
    "raw/fhir/condition",
    "logs",
    "config",
]
for f in folders:
    p = FILES_ROOT / f
    p.mkdir(parents=True, exist_ok=True)
    print(f"ready: {p}")

print("Attach lakehouse and run bronze→silver→gold notebooks next.")
print(f"Tables path hint: {TABLES_HINT}")
