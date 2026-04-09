# 📋 Méthodologie — DataFlow

## 1. Contexte et objectif

Ce projet est né d'un besoin concret en entreprise : les directions
financières consolident souvent leurs données manuellement depuis
plusieurs sources hétérogènes. L'objectif était de construire un
pipeline ETL (processus d'extraction, chargement et transformation )
complet et automatisable, avec une restitution visuelle
immédiate via un dashboard interactif.

## 2. Choix techniques et justifications

### Pourquoi DuckDB ?
DuckDB est une base de données analytique embarquée — elle s'installe
comme une simple librairie Python sans serveur à configurer. Elle est
conçue pour les requêtes analytiques sur des données tabulaires et est
jusqu'à 100x plus rapide que SQLite sur ce type de requêtes. C'est un
outil en forte croissance dans l'écosystème Data.

### Pourquoi 3 sources différentes ?
Simuler une vraie situation d'entreprise où les données viennent
rarement d'une source unique. CSV (export ERP), API (données externes
en temps réel), SQLite (base interne) couvrent les 3 cas les plus
fréquents en entreprise.

### Pourquoi Streamlit plutôt que Power BI ?
Streamlit permet de coder le dashboard en Python pur, de le connecter
directement à l'ETL, et de le déployer facilement sur le cloud. C'est
l'outil privilégié des Data Scientists et Data Engineers pour des
dashboards rapides à produire et maintenables.

### Pourquoi un fallback sur les taux de change ?
En production, une API peut être indisponible. Le mécanisme de fallback
(taux par défaut si l'API échoue) garantit que le pipeline ne plante
jamais — principe de résilience fondamental en Data Engineering.

## 3. Architecture ETL
```
CSV → extraire_ventes() ─────────────────────┐
API → extraire_taux_change() ─────────────────┼→ charger_dans_duckdb() → Dashboard
SQLite → extraire_budgets() ─────────────────┘
```

Chaque fonction a une responsabilité unique — principe de séparation
des responsabilités appliqué au Data Engineering.

## 4. Difficultés rencontrées

| Difficulté | Solution |
|---|---|
| DuckDB non installé | `python -m pip install duckdb` |
| Chemin relatif vers ventes.csv | Organisation claire src/ et data/ |
| API taux de change indisponible | Mécanisme fallback avec taux par défaut |
| Données brutes non lisibles | Section dépliable dans Streamlit |

## 5. Améliorations possibles

- Scheduler automatique (APScheduler) pour rafraîchir l'ETL toutes les heures
- Ajout d'une 4ème source : API météo pour croiser avec les ventes saisonnières
- Déploiement sur Streamlit Cloud pour accès sans installation locale
- Export PDF automatique du dashboard

## 6. Compétences développées

- Conception d'un pipeline ETL multi-sources de bout en bout
- Maîtrise de DuckDB comme base analytique embarquée
- Connexion et résilience sur une API publique externe
- Dashboard interactif avec rafraîchissement des données en temps réel
- Structuration professionnelle d'un projet Data Engineering
