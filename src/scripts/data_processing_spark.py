# ------------------------------ Importation des bibliothèques ------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when, to_date

# ------------------------------ Constantes ------------------------------
# chemin pour accéder aux données
FILE_PATH = './src/data/donnees_immobilieres.parquet'

# chemin pour sauvegarder les données nettoyées
OUTPUT_PATH = './src/data/donnees_immobilieres_cleaned.parquet'

# ------------------------------ Application ------------------------------
# initialisation SparkSession
spark = SparkSession.builder.appName("RealStateProcessing").getOrCreate()

# chargement des données
df_raw = spark.read.parquet(FILE_PATH)

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


# sauvegarde des données nettoyées au format Parquet
df.write.mode("overwrite").parquet(OUTPUT_PATH)