from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_date, when

FILE_PATH = "/src/data/raw/donnees_immobilieres.parquet"
OUTPUT_PATH = "/src/data/clean/donnees_immobilieres_cleaned.delta"

builder = (
    SparkSession.builder.appName("RealStateProcessing")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_raw = spark.read.parquet(FILE_PATH)

print("Début du nettoyage des données immobilières...")
print("Nombre d'enregistrements avant nettoyage :", df_raw.count())

df = df_raw.dropDuplicates(["id"])
df = df.filter(col("pays") == "France")

df = df.withColumn(
    "date_inventaire",
    to_date(col("date_inventaire"), "yyyy-MM")
)

df = df.withColumn("ville", lower(col("ville")))
df = df.withColumn("region", lower(col("region")))

df = df.withColumn(
    "ville",
    when(
        lower(col("ministere")).like("%intérieur%"),
        "Donnée confidentielle"
    ).otherwise(col("ville")),
)

df = df.withColumn(
    "code_postal",
    when(
        lower(col("ministere")).like("%intérieur%"),
        "Donnée confidentielle"
    ).otherwise(col("code_postal")),
)

df = df.withColumn(
    "adresse",
    when(
        lower(col("ministere")).like("%intérieur%"),
        "Donnée confidentielle"
    ).otherwise(col("adresse")),
)

df = (
    df.withColumn(
        "ministere",
        when(
            lower(col("ministere")).like("%intérieur%"),
            "ministère de l'Intérieur",
        ).otherwise(lower(col("ministere"))),
    )
    .withColumn(
        "ministere",
        when(
            lower(col("ministere")).like("%éducation%"),
            "ministère de l'Éducation Nationale",
        ).otherwise(col("ministere")),
    )
    .withColumn(
        "ministere",
        when(
            lower(col("ministere")).like("%justice%"),
            "ministère de la Justice",
        ).otherwise(col("ministere")),
    )
    .withColumn(
        "ministere",
        when(
            lower(col("ministere")).like("%santé%"),
            "ministère de la Santé",
        ).otherwise(col("ministere")),
    )
    .withColumn(
        "ministere",
        when(
            lower(col("ministere")).like("%ecologie%")
            | lower(col("ministere")).like("%écologie%")
            | lower(col("ministere")).like("%environnement%"),
            "ministère de l'Ecologie, développement et aménagement durables",
        ).otherwise(col("ministere")),
    )
)

df = df.fillna({
    "type": "inconnu",
    "region": "inconnu",
    "dept": "inconnu",
    "ministere": "inconnu",
})

print("Nombre d'enregistrements après nettoyage :", df.count())
print("Nettoyage terminé. Sauvegarde des données nettoyées au format Delta...")

Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)

df.write.format("delta").mode("overwrite").save(OUTPUT_PATH)

spark.stop()