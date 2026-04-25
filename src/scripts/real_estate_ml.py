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
from pyspark.sql.functions import col, lit

# ------------------------------ Constantes ------------------------------
# Le fichier d'entrée reste en Delta car c'est ta couche "Clean"
FILE_PATH = "/src/data/clean/donnees_immobilieres_cleaned.delta"

# Nouveaux chemins en .parquet pour Apache Druid
CLASSIFICATION_OUTPUT_PATH = "/src/data/ml/classification/ministere_predictions.parquet"
CLUSTERING_OUTPUT_PATH = "/src/data/ml/clustering/clusters.parquet"

FIGURES_OUTPUT_PATH = "/src/data/figures/"

# ------------------------------ Application ------------------------------
builder = (
    SparkSession.builder.appName("RealEstateML")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Charger les données (format Delta en entrée)
df = spark.read.format("delta").load(FILE_PATH)

# ------------- Classification -------------
df_ml = df.select("type", "fonction", "region", "dept", "ministere").dropna()

categorical_cols = ["type", "fonction", "region", "dept"]
indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
    for col in categorical_cols
]

label_indexer = StringIndexer(inputCol="ministere", outputCol="label", handleInvalid="keep")

encoder = OneHotEncoder(
    inputCols=[f"{col}_index" for col in categorical_cols],
    outputCols=[f"{col}_vec" for col in categorical_cols],
)

assembler = VectorAssembler(
    inputCols=[f"{col}_vec" for col in categorical_cols], outputCol="features"
)

rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50)

pipeline = Pipeline(stages=indexers + [label_indexer, encoder, assembler, rf])

train, test = df_ml.randomSplit([0.8, 0.2], seed=29)
model = pipeline.fit(train)
predictions = model.transform(test)

# Sélection et conversion pour Druid
# Note : probability est un vecteur, on le convertit en string pour Druid
predictions_to_save = predictions.select(
    "type", "region", "dept", "ministere", "prediction", 
    col("probability").cast("string").alias("probability")
)

# Sauvegarde en PARQUET
Path(CLASSIFICATION_OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
predictions_to_save.write.mode("overwrite").parquet(CLASSIFICATION_OUTPUT_PATH)

print(f"Classification terminée. Sauvegardée en Parquet ici : {CLASSIFICATION_OUTPUT_PATH}")

# ------------- Clustering -------------
df_cluster = df.select("type", "fonction", "region", "dept", "ministere", "date_inventaire").dropna()

categorical_cols = ["type", "fonction", "region", "dept", "ministere"]
indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index") for col in categorical_cols]

encoder = OneHotEncoder(
    inputCols=[f"{col}_index" for col in categorical_cols],
    outputCols=[f"{col}_vec" for col in categorical_cols],
)

assembler = VectorAssembler(inputCols=[f"{col}_vec" for col in categorical_cols], outputCol="features")

kmeans = KMeans(k=5, seed=29)
pipeline_cluster = Pipeline(stages=indexers + [encoder, assembler, kmeans])
model_cluster = pipeline_cluster.fit(df_cluster)

cluster_predictions = model_cluster.transform(df_cluster)

# Sélection des colonnes pour Druid (J'ajoute date_inventaire car Druid en a besoin)
cluster_to_save = cluster_predictions.select(
    "date_inventaire", "type", "region", "dept", "ministere", "prediction", lit(1).alias("quantite")
)

# Sauvegarde en PARQUET
Path(CLUSTERING_OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
cluster_to_save.write.mode("overwrite").parquet(CLUSTERING_OUTPUT_PATH)

print(f"Clustering terminé. Sauvegardé en Parquet ici : {CLUSTERING_OUTPUT_PATH}")