
from pathlib import Path
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Steam Gold Export")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "data" / "gold"
OUTPUT = ROOT / "dashboard"

OUTPUT.mkdir(exist_ok=True)

tables = [
    "games_stats",
    "language_stats",
    "playtime_stats"
]

for name in tables:
    df = spark.read.parquet(str(GOLD / name))

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(str(OUTPUT / name))

    print(f"Export OK : {name}", flush=True)

spark.stop()
