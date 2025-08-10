# ------------------------------ Importation des bibliothèques ------------------------------
import requests
import time
import pandas as pd

# ------------------------------ Fonctions ------------------------------
def get_all_real_estate_data(limit=50, max_records=1000):
    """
    Récupère l'ensemble des enregistrements de l'inventaire immobilier de l'État
    en gérant la pagination et en limitant le nombre total d'enregistrements.

    Args:
        limit (int): Le nombre d'enregistrements par page (par défaut 50).
        max_records (int): Le nombre maximum d'enregistrements à récupérer (par défaut 1000).
    
    Returns:
        list: Une liste contenant les enregistrements récupérés, jusqu'à la limite spécifiée.
    """
    all_records = []
    offset = 0
    page_number = 1
    
    # URL de l'API avec le jeu de données de l'inventaire immobilier de l'État
    url = "https://www.data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/inventaire-immobilier-de-letat/records"
    
    while True:
        print(f"Récupération de la page {page_number} (offset: {offset})...")
        
        # Arrêter si le nombre d'enregistrements récupérés dépasse la limite
        if len(all_records) >= max_records:
            print("Limite d'enregistrements atteinte. Arrêt de la récupération.")
            break
        
        params = {
            'limit': limit,
            'offset': offset,
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status() # Lève une exception pour les erreurs HTTP
            
            data = response.json()
            records = data.get('results', [])
            
            # Si la liste des enregistrements est vide, c'est la fin des données
            if not records:
                print("Toutes les pages ont été récupérées.")
                break
            
            all_records.extend(records)
            
            # Incrémenter l'offset pour la prochaine page
            offset += limit
            page_number += 1
            
            # Pause pour ne pas surcharger le serveur de l'API (bonne pratique)
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"Une erreur est survenue lors de la requête : {e}")
            break
    
    # S'assurer que le nombre final ne dépasse pas la limite
    return all_records[:max_records]

# ------------------------------ Utilisation du script ------------------------------
# Exécuter la fonction pour récupérer les 1000 premiers enregistrements
print("Début de la récupération des données immobilières de l'État...")
# On peut aussi appeler la fonction comme ceci : get_all_real_estate_data(max_records=5000) pour récupérer 5000 enregistrements
all_data = get_all_real_estate_data(limit=50)

# Afficher quelques informations sur les données récupérées
if all_data:
    print(f"\nRécupération terminée. Nombre total d'enregistrements : {len(all_data)}")
    
    # Création d'un DataFrame à partir de la liste de dictionnaires
    df = pd.DataFrame(all_data)
    
    # Affichage du DataFrame avant la sauvegarde
    print("\n--- Aperçu du DataFrame ---")
    print(df.head())
    
    # Enregistrement des données dans un fichier Parquet
    print("\nSauvegarde des données au format Parquet...")
    
    # Chemin de sauvegarde dans le conteneur (qui est monté sur votre machine)
    output_path = '/src/data/donnees_immobilieres.parquet'
    
    # Enregistrement du DataFrame dans un fichier Parquet
    df.to_parquet(output_path, index=False)
    print(f"Sauvegarde terminée. Fichier disponible à l'emplacement : {output_path}")