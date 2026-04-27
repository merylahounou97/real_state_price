# 🏡 Real Estate Prices ETL

Un projet Data Engineering complet qui couvre toutes les étapes d’un pipeline moderne de traitement de données, depuis le **web scraping** jusqu’à la **visualisation interactive de la classification et du clustering des biens immobiliers**.

Ce projet a pour double objectif :
- 🚀 **Monter en compétences** sur les outils les plus utilisés en ingénierie des données.
- 🏗️ **Construire une stack de production** réaliste et maintenable.

---

## 🌐 Vue d’ensemble du projet

L’objectif est de construire un **système automatisé, orchestré et conteneurisé** qui gère le cycle de vie complet de la donnée immobilière.

- 🕸 **Scrape** : Extraction automatisée des prix et caractéristiques (BeautifulSoup).
- ⚙️ **ETL & Data Lake** : Nettoyage, structuration et stockage ACID avec **PySpark** dans des **Delta Tables**.
- 🧠 **Machine Learning** : Entraînement d'un modèle de classification et de clustering via **Spark MLlib**.
- 🗃️ **Injest Analytics** : Chargement des prédictions dans **Apache Druid** pour des requêtes analytiques ultra-rapides.
- 📊 **Visualise** : Dashboard interactif sur **Apache Superset** pour explorer les résultats.
- 🪄 **Orchestrate** : Planification et supervision de toutes les tâches avec **Apache Airflow**.

---

## 🗺️ Architecture du Pipeline

L'ensemble de l'infrastructure est déployé de manière isolée via **Docker Compose**. Le diagramme ci-dessous illustre le flux de données et l'interaction entre les différents services conteneurisés.

![Diagramme de l'Architecture Modern Data Stack - Real Estate ](architecture_real_state.png)

**Flux de données détaillé (basé sur le diagramme) :**

1.  **Application (IMMOBILIER-APP) :** Le cœur fonctionnel.
    * `scraping.py` extrait les données.
    * `PySpark` effectue le Spark Processing.
    * `Spark MLlib` exécute le clustering et la classification.
2.  **Shared Storage (Docker Volumes) :** La persistance des données. Les scripts lisent et écrivent dans des **Delta Tables** (Data Lake) partagées entre les conteneurs (`raw_delta_data`, `clean_delta_data`).
3.  **Data Ingestion (HTTP API) :** Une fois le modèle entraîné, les prédictions sont envoyées à **Apache Druid** via une requête `POST` sur son API d'ingestion.
4.  **Analytics & Visualization :**
    * **Druid** (Coordonnateur, Historique, Courtier) indexe les données.
    * **Superset** se connecte à Druid via des requêtes SQL pour alimenter le Dashboard final.

---

## 💡 Technologies utilisées

| 🧩 Catégorie        | 🔧 Technologie             | 📘 Description |
|--------------------|----------------------------|----------------|
| **Web Scraping**   | Python                     | Langage principal du projet |
|                    | BeautifulSoup & Requests   | Extraction HTML rapide et simple |
| **ETL / Traitement** | Apache PySpark             | Traitement distribué de données massives |
| **Stockage brut & Data Lake** | Delta Lake           | Couches ACID sur fichiers parquet |
| **Machine Learning** | PySpark MLlib              | ML intégré à Spark pour la scalabilité |
| **Data Warehouse** | Apache Druid               | Base analytique OLAP rapide & temps réel |
| **Orchestration**  | Apache Airflow             | Orchestration des tâches ETL/ML |
| **Conteneurisation** | Docker & Docker Compose    | Déploiement local multi-service |
| **Visualisation**  | Apache Superset            | Dashboards interactifs open-source |


# 🚀 Lancer l'application

## 📦 Prérequis

- Docker
- Docker Compose

---

## 🐳 1. Build de l'image applicative

```bash
docker build -t immobilier-app:latest .
```

---

## ⚙️ 2. Initialisation de la stack

```bash
bash setup.sh
```

### 🔧 Ce que fait le script :

- Démarre PostgreSQL, Zookeeper, Druid et Airflow
- Crée les bases `superset` et `airflow`
- Configure Airflow (Docker socket)
- Attend que Druid (Overlord) soit prêt

### 🔗 Accès aux services

| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | admin / admin |
| Druid   | http://localhost:8888 | — |

---

## 🔄 3. Lancer le pipeline Airflow

1. Ouvrir Airflow :

```
http://localhost:8080
```

2. Activer le DAG :

```
immobilier_pipeline.py
```

3. Lancer le DAG manuellement via l'interface

### 📊 Étapes du pipeline :

- Scraping
- Nettoyage Spark
- Transformation Delta Lake
- Clustering / Classification
- Ingestion Druid
- Vérification de l'ingestion

---

## 📊 4. Vérifier les données dans Druid

```bash
curl http://localhost:8081/druid/coordinator/v1/datasources
```

**Résultat attendu :**

```json
["real_estate_clusters"]
```

---

## 📊 5. Lancer Superset

```bash
bash start_superset.sh
```

### 🔗 Accès Superset

```
http://localhost:8088
```

**Login :**

```
admin / admin
```

---

## 🔌 6. Connecter Druid à Superset

Dans Superset, naviguer vers :

```
Settings → Database Connections → + Database
```

Utiliser l'URI suivante :

```
druid://druid:8082/druid/v2/sql
```

Puis :

- ✅ Tester la connexion
- 💾 Sauvegarder

---

## 📈 7. Créer les visualisations

Créer un dataset à partir de :

```
real_estate_clusters
```

Vous pouvez créer vos graphiques maintenant

## 🧹 Arrêter la stack

```bash
docker compose down
```

Avec suppression des volumes :

```bash
docker compose down -v
```