
from airflow.sdk import dag, task, get_current_context
from datetime import datetime
from pathlib import Path
import subprocess
import sys

ROOT = Path("/opt/steam")
DATA = ROOT / "data"


def run_script(script):
    print(f"Exécution de {script}", flush=True)
    subprocess.run(
        [sys.executable, str(ROOT / "src" / script)],
        cwd=str(ROOT),
        check=True
    )


def rebuild_enabled():
    return get_current_context()["params"]["rebuild"]


@dag(
    dag_id="steam_bigdata_pipeline",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"rebuild": False},
    tags=["steam", "pyspark", "medallion"]
)
def steam_pipeline():

    @task
    def bronze():
        files = list((DATA / "bronze").glob("*.parquet"))

        if not files:
            raise FileNotFoundError("Aucun fichier Bronze trouvé")

        print(f"Bronze OK : {len(files)} fichiers", flush=True)

        if rebuild_enabled():
            run_script("download_data.py")

    @task
    def silver():
        path = DATA / "silver" / "steam_reviews"

        if rebuild_enabled():
            run_script("bronze_to_silver.py")

        if not (path / "_SUCCESS").exists():
            raise FileNotFoundError("Silver incomplet ou absent")

        print("Silver OK : données disponibles", flush=True)

    @task
    def gold():
        tables = ["games_stats", "language_stats", "playtime_stats"]

        if rebuild_enabled():
            run_script("silver_to_gold.py")

        for name in tables:
            path = DATA / "gold" / name
            if not (path / "_SUCCESS").exists():
                raise FileNotFoundError(f"Gold incomplet : {name}")

            print(f"Gold OK : {name}", flush=True)

    @task
    def data_quality():
        run_script("data_quality_gold.py")
        
    bronze() >> silver() >> gold() >> data_quality()


steam_pipeline()
