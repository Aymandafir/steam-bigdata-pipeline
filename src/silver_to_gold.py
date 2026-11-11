
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum,
    round as spark_round, when
)

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

# Lire les données Silver
df = spark.read.parquet("data/silver/steam_reviews")

# Convertir le booléen en 1 ou 0
df = df.withColumn(
    "positive",
    when(col("voted_up") == 1, 1).otherwise(0)
)

# ANALYSE 1 : Popularité des jeux
games = (
    df.groupBy("appid", "game")
    .agg(
        count("*").alias("total_reviews"),
        spark_round(
            avg("positive") * 100, 2
        ).alias("positive_rate")
    )
)

games.write.mode("overwrite").parquet(
    "data/gold/games_stats"
)

print("\n===== TOP JEUX PAR REVIEWS =====")
games.orderBy(col("total_reviews").desc()).show(10)

# ANALYSE 2 : Langues
languages = (
    df.groupBy("language")
    .agg(
        count("*").alias("total_reviews")
    )
)

languages.write.mode("overwrite").parquet(
    "data/gold/language_stats"
)

print("\n===== LANGUES =====")
languages.orderBy(
    col("total_reviews").desc()
).show(10)

# ANALYSE 3 : Temps de jeu
df = df.withColumn(
    "playtime_hours",
    col("author_playtime_forever") / 60
)

playtime = (
    df.withColumn(
        "playtime_group",
        when(col("playtime_hours") < 10, "0-10h")
        .when(col("playtime_hours") < 100, "10-100h")
        .when(col("playtime_hours") < 500, "100-500h")
        .otherwise("500h+")
    )
    .groupBy("playtime_group")
    .agg(
        count("*").alias("total_reviews"),
        spark_round(
            avg("positive") * 100, 2
        ).alias("positive_rate")
    )
)

playtime.write.mode("overwrite").parquet(
    "data/gold/playtime_stats"
)

print("\n===== TEMPS DE JEU =====")
playtime.orderBy("playtime_group").show()

spark.stop()
