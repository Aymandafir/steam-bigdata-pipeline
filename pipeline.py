
import subprocess
import sys
import time
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent

STEPS = [
    ("Bronze - Ingestion", "src/download_data.py"),
    ("Silver - Nettoyage", "src/bronze_to_silver.py"),
    ("Gold - Analyse", "src/silver_to_gold.py"),
    ("Data Quality", "src/data_quality_gold.py"),
]

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run",
    choices=["all", "silver", "gold"],
    default="gold"
)
args = parser.parse_args()

if args.run == "all":
    selected = STEPS
elif args.run == "silver":
    selected = STEPS[1:]
else:
    selected = STEPS[2:]

start_pipeline = time.perf_counter()

for name, script in selected:
    print(f"\n===== {name} =====", flush=True)

    start = time.perf_counter()

    result = subprocess.run(
        [sys.executable, str(ROOT / script)],
        cwd=ROOT
    )

    duration = time.perf_counter() - start

    print(
        f"{name} : {duration:.1f} secondes",
        flush=True
    )

    if result.returncode != 0:
        print(f"ERREUR : {name}")
        sys.exit(result.returncode)

print("\n===== PIPELINE TERMINÉ =====")
print(
    f"Durée totale : "
    f"{(time.perf_counter() - start_pipeline) / 60:.2f} min"
)
