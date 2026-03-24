# ------------------------------ Importation des bibliothèques ------------------------------
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when, to_date
from delta import configure_spark_with_delta_pip

# ------------------------------ Constantes ------------------------------
# chemin pour accéder aux données
FILE_PATH = './src/data/raw/donnees_immobilieres.parquet'

# chemin pour sauvegarder les données nettoyées
OUTPUT_PATH = './src/data/clean/donnees_immobilieres_cleaned.delta'

# ------------------------------ Application ------------------------------
# initialisation SparkSession
builder = (
    SparkSession.builder
    .appName("RealStateProcessing")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# chargement des données
df_raw = spark.read.parquet(FILE_PATH)

print("Début du nettoyage des données immobilières...")
print("Nombre d'enregistrements avant nettoyage :", df_raw.count())

# suppression des doublons suivant l'id
df = df_raw.dropDuplicates(['id'])

# filtrage pour ne garder que les biens en France
df = df.filter(col("pays") == "France")

# conversion de la colonne "date_invnetaire" en type date
df = df.withColumn("date_inventaire", to_date(col("date_inventaire"), "yyyy-MM"))

# mise en minuscule de la colonne "ville" pour uniformiser les données
df = df.withColumn("ville", lower(col("ville")))

# mise en minuscule de la colonne "region" pour uniformiser les données
df = df.withColumn("region", lower(col("region")))

# anonymisation des données sensibles
df = df.withColumn("ville",when(col("ministere") == "ministère de l'Intérieur", "Donnée confidentielle").otherwise(col("ville")))

# anonymisation des données sensibles
df = df.withColumn("code_postal", when(col("ministere") == "ministère de l'Intérieur", "Donnée confidentielle").otherwise(col("code_postal")))

# anonymisation des données sensibles
df = df.withColumn("adresse", when(col("ministere") == "ministère de l'Intérieur", "Donnée confidentielle").otherwise(col("adresse")))

# uniformisation des données du ministère
df = (df
    .withColumn("ministere", when(lower(col("ministere")).like("%intérieur%"), "ministère de l'Intérieur").otherwise(lower(col("ministere"))))
    .withColumn("ministere", when(lower(col("ministere")).like("%éducation%"), "ministère de l'Éducation Nationale").otherwise(col("ministere")))
    .withColumn("ministere", when(lower(col("ministere")).like("%justice%"), "ministère de la Justice").otherwise(col("ministere")))
    .withColumn("ministere", when(lower(col("ministere")).like("%santé%"), "ministère de la Santé").otherwise(col("ministere")))
    .withColumn("ministere", when(lower(col("ministere")).like("%ecologie%") | lower(col("ministere")).like("%environnement%"), "ministère de l'Ecologie, développement et aménagement durables").otherwise(col("ministere")))
)


print("Nombre d'enregistrements après nettoyage :", df.count())
print("Nettoyage terminé. Sauvegarde des données nettoyées au format Delta...")

# Création du dossier si il n'existe pas
Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)

# sauvegarde des données nettoyées au format delta
df.write.format("delta").mode("overwrite").save(OUTPUT_PATH)