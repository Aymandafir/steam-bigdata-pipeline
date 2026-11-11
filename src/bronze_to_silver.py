
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Steam Big Data Pipeline")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 1. Lire Bronze
df = spark.read.parquet(
    "data/bronze/*.parquet"
)

# 2. Sélectionner les colonnes utiles
df_silver = df.select(
    "recommendationid",
    "appid",
    "game",
    "author_steamid",
    "author_playtime_forever",
    "language",
    "review",
    "timestamp_created",
    "voted_up",
    "votes_up"
)

# 3. Nettoyer les données
df_silver = (
    df_silver
    .filter(col("recommendationid").isNotNull())
    .filter(col("appid").isNotNull())
    .filter(col("author_playtime_forever") >= 0)
    .dropDuplicates(["recommendationid"])
)

# 4. Enregistrer Silver
df_silver.write.mode("overwrite").parquet(
    "data/silver/steam_reviews"
)

print("===== SILVER CRÉÉ =====")
print("Total reviews :", df_silver.count())

spark.stop()
