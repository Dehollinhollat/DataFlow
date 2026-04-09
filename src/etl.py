"""
etl.py — Pipeline ETL de DataFlow
----------------------------------
Ce fichier orchestre l'extraction, la transformation et le chargement
des données depuis 3 sources différentes vers une base DuckDB centrale.

ETL = Extract (extraire) → Transform (transformer) → Load (charger)

Auteur : Déhollin HOLLAT — Chef de Projet Data IA
"""

import pandas as pd      # Manipulation de données tabulaires
import duckdb            # Base de données analytique ultra-rapide
import sqlite3           # Base de données SQLite (budgets)
import requests          # Appels HTTP vers des APIs externes
import os                # Gestion des fichiers et dossiers
from datetime import datetime  # Gestion des dates et heures

# Création du dossier data/ s'il n'existe pas déjà
os.makedirs("data", exist_ok=True)


def extraire_ventes():
    """
    SOURCE 1 — Fichier CSV local
    ----------------------------
    Lit le fichier ventes.csv qui contient toutes les transactions
    de vente de l'entreprise.

    Transformations appliquées :
    - Conversion de la colonne 'date' en format datetime (de texte → date)
    - Calcul du total par ligne : quantite × prix_unitaire

    Retourne : DataFrame pandas avec les ventes enrichies
    """
    df = pd.read_csv("ventes.csv")

    # Conversion de la date en format exploitable (ex: "2024-01-05" → datetime)
    df["date"] = pd.to_datetime(df["date"])

    # Calcul du montant total par transaction
    df["total"] = df["quantite"] * df["prix_unitaire"]

    print(f"Ventes extraites : {len(df)} lignes")
    return df


def extraire_taux_change():
    """
    SOURCE 2 — API publique de taux de change
    ------------------------------------------
    Interroge l'API gratuite exchangerate-api.com pour récupérer
    les taux de change en temps réel par rapport à l'Euro (EUR).

    Taux récupérés :
    - EUR/USD : Euro vers Dollar américain
    - EUR/GBP : Euro vers Livre sterling
    - EUR/CHF : Euro vers Franc suisse

    En cas d'échec de l'API (pas de connexion, limite atteinte...),
    des taux par défaut sont utilisés pour ne pas bloquer le pipeline.

    Retourne : dictionnaire avec les taux et la date de mise à jour
    """
    try:
        # Appel à l'API — récupère tous les taux depuis l'Euro
        response = requests.get("https://api.exchangerate-api.com/v4/latest/EUR")
        data = response.json()

        taux = {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "EUR_USD": data["rates"]["USD"],
            "EUR_GBP": data["rates"]["GBP"],
            "EUR_CHF": data["rates"]["CHF"],
        }
        print(f"Taux de change recuperes : EUR/USD = {taux['EUR_USD']}")

    except Exception as e:
        # Fallback : si l'API est indisponible, on utilise des taux fixes
        # Le pipeline continue de fonctionner sans interruption
        print(f"API indisponible, taux par defaut utilises : {e}")
        taux = {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "EUR_USD": 1.08,
            "EUR_GBP": 0.86,
            "EUR_CHF": 0.95
        }

    return taux


def extraire_budgets():
    """
    SOURCE 3 — Base de données SQLite
    -----------------------------------
    Lit la table 'budgets' depuis la base SQLite créée par setup_db.py.
    Contient le budget annuel et le budget déjà consommé par département.

    Transformations appliquées :
    - Calcul du budget restant : budget_annuel - budget_consomme
    - Calcul du taux de consommation en % : budget_consomme / budget_annuel × 100

    Retourne : DataFrame pandas avec les budgets enrichis
    """
    # Connexion à la base SQLite
    conn = sqlite3.connect("data/dataflow.db")

    # Lecture de toute la table budgets via une requête SQL
    df = pd.read_sql("SELECT * FROM budgets", conn)
    conn.close()

    # Calcul des indicateurs dérivés
    df["budget_restant"] = df["budget_annuel"] - df["budget_consomme"]
    df["taux_consommation"] = round(
        df["budget_consomme"] / df["budget_annuel"] * 100, 1
    )

    print(f"Budgets extraits : {len(df)} departements")
    return df


def charger_dans_duckdb(df_ventes, df_budgets, taux):
    """
    CHARGEMENT — Base de données DuckDB
    -------------------------------------
    DuckDB est une base de données analytique embarquée, très rapide
    pour les requêtes sur de gros volumes de données.

    On y charge les 3 sources transformées :
    - Table 'ventes' : toutes les transactions de vente
    - Table 'budgets' : budgets par département
    - Table 'taux_change' : taux de change du jour

    Les tables sont recréées à chaque exécution (DROP + CREATE)
    pour toujours avoir des données fraîches.
    """
    # Connexion à DuckDB (crée le fichier s'il n'existe pas)
    conn = duckdb.connect("data/dataflow.duckdb")

    # Chargement des ventes
    # DROP TABLE supprime l'ancienne version pour éviter les doublons
    conn.execute("DROP TABLE IF EXISTS ventes")
    conn.execute("CREATE TABLE ventes AS SELECT * FROM df_ventes")

    # Chargement des budgets
    conn.execute("DROP TABLE IF EXISTS budgets")
    conn.execute("CREATE TABLE budgets AS SELECT * FROM df_budgets")

    # Chargement des taux de change
    # On construit la table directement depuis le dictionnaire Python
    conn.execute("DROP TABLE IF EXISTS taux_change")
    conn.execute(f"""
        CREATE TABLE taux_change AS
        SELECT
            '{taux['date']}' as date_maj,
            {taux['EUR_USD']} as EUR_USD,
            {taux['EUR_GBP']} as EUR_GBP,
            {taux['EUR_CHF']} as EUR_CHF
    """)

    conn.close()
    print("Donnees chargees dans DuckDB")


def run_etl():
    """
    ORCHESTRATEUR — Fonction principale du pipeline ETL
    ----------------------------------------------------
    Appelle les 3 fonctions d'extraction dans l'ordre,
    puis charge toutes les données dans DuckDB.

    C'est cette fonction qui est appelée au lancement du script
    et qui sera aussi appelée par le dashboard Streamlit
    pour rafraîchir les données.
    """
    print("=== DEBUT ETL ===")

    # Étape 1 — Extraction des 3 sources
    df_ventes = extraire_ventes()
    taux = extraire_taux_change()
    df_budgets = extraire_budgets()

    # Étape 2 — Chargement dans DuckDB
    charger_dans_duckdb(df_ventes, df_budgets, taux)

    print("=== ETL TERMINE ===")


# Point d'entrée du script
# Ce bloc ne s'exécute que si on lance directement : python etl.py
# Il ne s'exécute pas si etl.py est importé dans un autre fichier
if __name__ == "__main__":
    run_etl()
