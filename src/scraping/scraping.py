# ------------------------------ Importation des bibliothèques ------------------------------
import requests
import time
import pandas as pd


# ------------------------------ Fonctions ------------------------------
def get_all_real_estate_data(limit=50):
    """
    Récupère l'ensemble des enregistrements de l'inventaire immobilier de l'État
    en gérant la pagination.

    Args:
        limit (int): Le nombre d'enregistrements par page (par défaut 50).
    
    Returns:
        list: Une liste contenant tous les enregistrements récupérés.
    """
    all_records = []
    offset = 0
    page_number = 1
    
    # URL de l'API avec le jeu de données de l'inventaire immobilier de l'État
    url = "https://www.data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/inventaire-immobilier-de-letat/records"
    
    while True:
        print(f"Récupération de la page {page_number} (offset: {offset})...")
        
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
    
    return all_records

# ------------------------------ Utilisation du script ------------------------------
# Exécuter la fonction pour récupérer toutes les données
print("Début de la récupération de toutes les données immobilières de l'État...")
all_data = get_all_real_estate_data(limit=50)

# Afficher quelques informations sur les données récupérées
if all_data:
    print(f"\nRécupération terminée. Nombre total d'enregistrements : {len(all_data)}")
    
    # Afficher les 5 premiers enregistrements pour vérification
    print("\n--- Les 5 premiers enregistrements ---")
    for i, record in enumerate(all_data[:5]):
        print(f"[{i+1}] ID : {record.get('id')}, Type de bien : {record.get('type_de_bien')}, Région : {record.get('region_1_nom')}")

    # Enregistrement des données dans un fichier CSV
    print("\nSauvegarde des données au format CSV...")
    
    
    # Création d'un DataFrame à partir de la liste de dictionnaires
    df = pd.DataFrame(all_data)
    
    # Chemin de sauvegarde dans le conteneur (qui est monté sur votre machine)
    output_path = '/src/data/donnees_immobilieres.csv'
    
    # Enregistrement du DataFrame dans un fichier CSV
    df.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"Sauvegarde terminée. Fichier disponible à l'emplacement : {output_path}")
    