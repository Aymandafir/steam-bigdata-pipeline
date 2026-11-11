
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Steam Data Quality")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

root = Path("/opt/steam/data/gold")

tables = {
    "games_stats": ["game", "total_reviews"],
    "language_stats": ["language", "total_reviews"],
    "playtime_stats": ["total_reviews"],
}

for name, required_columns in tables.items():
    path = str(root / name)
    df = spark.read.parquet(path)

    if not set(required_columns).issubset(df.columns):
        raise ValueError(f"{name}: colonnes manquantes")

    invalid = df.filter(
        col("total_reviews").isNull() |
        (col("total_reviews") < 0)
    ).limit(1).count()

    if invalid:
        raise ValueError(f"{name}: total_reviews invalide")

    print(f"DATA QUALITY OK : {name}", flush=True)

spark.stop()
