# ------------------------------ Importation des bibliothèques ------------------------------
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.clustering import KMeans
from pathlib import Path

# ------------------------------ Constantes ------------------------------
# chemin pour accéder aux données
FILE_PATH = "./src/data/clean/donnees_immobilieres_cleaned.delta"

# chemin pour sauvegarder les résultats
CLASSIFICATION_OUTPUT_PATH = "./src/data/ml/classification/ministere_predictions.delta"
CLUSTERING_OUTPUT_PATH = "./src/data/ml/clustering/clusters.delta"

# chemin pour sauvegarder les figures
FIGURES_OUTPUT_PATH = "./src/data/figures/"

# ------------------------------ Application ------------------------------
# Configuer sparksession pour utiliser delta lake
builder = (
    SparkSession.builder.appName("RealEstateML")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Charger les données
df = spark.read.format("delta").load(FILE_PATH)

# ------------- Classification -------------
# Sélection des colonnes pertinentes et suppression des lignes avec des valeurs manquantes
df_ml = df.select("type", "fonction", "region", "dept", "ministere").dropna()

# Indexer pour les variables catégorielles
categorical_cols = ["type", "fonction", "region", "dept"]
indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
    for col in categorical_cols
]

# Indexer pour la variable cible
label_indexer = StringIndexer(
    inputCol="ministere", outputCol="label", handleInvalid="keep"
)

# OneHotEncoder pour les variables catégorielles
encoder = OneHotEncoder(
    inputCols=[f"{col}_index" for col in categorical_cols],
    outputCols=[f"{col}_vec" for col in categorical_cols],
)

# Assemblage des features
assembler = VectorAssembler(
    inputCols=[f"{col}_vec" for col in categorical_cols], outputCol="features"
)

# Création du modèle de classification
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50)

# Création du pipeline
pipeline = Pipeline(stages=indexers + [label_indexer, encoder, assembler, rf])

# Split des données en train et test
train, test = df_ml.randomSplit([0.8, 0.2], seed=29)

# Entraînement du modèle
model = pipeline.fit(train)

# Prédictions sur le jeu de test
predictions = model.transform(test)

# Calcul de l'accuracy
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)

# Calcul de l'accuracy
accuracy = evaluator.evaluate(predictions)
print(f"Accuracy : {accuracy}")

# Création du dossier si il n'existe pas
Path(CLASSIFICATION_OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)

# Sélection des colonnes à sauvegarder
predictions = predictions.select(
    "type", "region", "dept", "ministere", "prediction", "probability"
)

# Sauvegarde des prédictions de classification
predictions.write.format("delta").mode("overwrite").save(CLASSIFICATION_OUTPUT_PATH)

print("Classification terminée et sauvegardée avec succès !")

# ------------- Clustering -------------
# Sélection des colonnes pertinentes pour le clustering et suppression des lignes avec des valeurs manquantes
df_cluster = df.select("type", "fonction", "region", "dept", "ministere").dropna()

# Indexer pour les variables catégorielles
categorical_cols = ["type", "fonction", "region", "dept", "ministere"]

indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_index") for col in categorical_cols
]

# OneHotEncoder pour les variables catégorielles
encoder = OneHotEncoder(
    inputCols=[f"{col}_index" for col in categorical_cols],
    outputCols=[f"{col}_vec" for col in categorical_cols],
)

# Assemblage des features
assembler = VectorAssembler(
    inputCols=[f"{col}_vec" for col in categorical_cols], outputCol="features"
)

# Création du modèle de clustering
kmeans = KMeans(k=5, seed=29)

# Création du pipeline
pipeline = Pipeline(stages=indexers + [encoder, assembler, kmeans])

# Entraînement du modèle de clustering
model = pipeline.fit(df_cluster)

# Prédictions de clustering
cluster_predictions = model.transform(df_cluster)

# Affichage des prédictions de clustering
cluster_predictions.select("type", "region", "prediction").show()

# Création du dossier si il n'existe pas
cluster_predictions = cluster_predictions.select(
    "type", "region", "dept", "ministere", "prediction"
)

# Sauvegarde des prédictions de clustering
Path(CLUSTERING_OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)

# Sauvegarde des prédictions de clustering
cluster_predictions.write.format("delta").mode("overwrite").save(CLUSTERING_OUTPUT_PATH)

print("Clustering terminé et sauvegardé avec succès !")
