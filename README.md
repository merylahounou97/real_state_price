# 🏡 Real Estate Prices ETL

Un projet Data Engineering complet qui couvre toutes les étapes d’un pipeline moderne de traitement de données, depuis le **web scraping** jusqu’à la **visualisation interactive des prédictions de prix immobiliers**.

Ce projet a pour double objectif :
- 🚀 **Monter en compétences** sur les outils les plus utilisés en ingénierie des données

---

## 🌐 Vue d’ensemble du projet

L’objectif est de construire un **système automatisé et conteneurisé** qui :

- 🕸 **Scrape** les prix et caractéristiques de biens immobiliers depuis des sites web
- ⚙️ **Intègre, nettoie et transforme** les données avec **PySpark**
- 💾 **Stocke** les données structurées dans des **Delta Tables** (Data Lake ACID)
- 🧠 **Entraîne un modèle de ML** pour prédire les prix via **Spark MLlib**
- 🗃️ **Injeste** les données enrichies dans **Apache Druid**, base analytique en temps réel
- 📊 **Visualise** les résultats et la performance du modèle dans un dashboard via **Apache Superset**
- 🪄 **Orchestre** toutes les tâches avec **Apache Airflow**
- 🐳 **Conteneurise** le projet avec **Docker**, pour une portabilité totale

---

## 💡 Technologies utilisées

Le projet repose sur une stack moderne et open-source, utilisée en entreprise dans des pipelines data à grande échelle.

| 🧩 Catégorie        | 🔧 Technologie             | 📘 Description |
|--------------------|----------------------------|----------------|
| **Web Scraping**   | Python                     | Langage principal du projet |
|                    | BeautifulSoup & Requests   | Extraction HTML rapide et simple |
| **ETL / Traitement** | Apache PySpark             | Traitement distribué de données massives |
| **Stockage brut & Data Lake** | Delta Lake           | Couches ACID sur fichiers parquet |
| **Machine Learning** | PySpark MLlib              | ML intégré à Spark pour la scalabilité |
| **Data Warehouse** | Apache Druid               | Base analytique OLAP rapide & temps réel |
| **Orchestration**  | Apache Airflow             | Orchestration des tâches ETL/ML |
| **Conteneurisation** | Docker & Docker Compose    | Déploiement local multi-service |
| **Visualisation**  | Apache Superset            | Dashboards interactifs open-source |

---

## 🗺️ Architecture du pipeline

```text
[Scraping Web]
     ↓
[Stockage brut (MinIO ou local)]
     ↓
[Nettoyage & Transformation avec PySpark]
     ↓
[Delta Tables]
     ↓
[Modèle de ML - prédictions]
     ↓
[Stockage des prédictions dans Apache Druid]
     ↓
[Dashboard Superset pour exploration]
