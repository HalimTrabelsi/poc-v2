#!/usr/bin/env python
"""
Exécuter le notebook d'analyse des modèles sans Jupyter.

Usage:
    cd fraud-detection-engine
    python ml/notebooks/run_notebook.py
"""

import json
import sys
from pathlib import Path

# Charger le notebook
nb_path = Path(__file__).parent / "modeles_fraude_analyse.ipynb"
with open(nb_path) as f:
    nb = json.load(f)

# Extraire le code des cellules
code_cells = [
    "".join(cell["source"])
    for cell in nb["cells"]
    if cell["cell_type"] == "code"
]

# Assembler
full_code = "\n\n".join(code_cells)

# Exécuter
print(f"Exécution du notebook {nb_path.name}...")
print("=" * 60)
try:
    exec(full_code, {"__name__": "__main__"})
    print("=" * 60)
    print("✅ Analyse terminée avec succès!")
except Exception as e:
    print(f"❌ Erreur lors de l'exécution : {e}", file=sys.stderr)
    import traceback
    traceback.print_exc()
    sys.exit(1)
