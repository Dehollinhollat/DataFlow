"""
dashboard.py — Dashboard temps réel DataFlow
---------------------------------------------
Interface Streamlit qui visualise les données consolidées
par le pipeline ETL (etl.py).

Le dashboard se rafraîchit automatiquement toutes les 60 secondes
pour toujours afficher les données les plus récentes.

Auteur : Déhollin HOLLAT — Chef de Projet Data IA
"""

import streamlit as st  # Framework dashboard Python
import duckdb           # Connexion à la base analytique
import pandas as pd     # Manipulation des données
from etl import run_etl # Import du pipeline ETL

# ─── CONFIGURATION DE LA PAGE ───────────────────────────────
st.set_page_config(
    page_title="DataFlow — Dashboard Financier",
    page_icon="🟢",
    layout="wide"
)

st.title("🟢 DataFlow — Dashboard Financier Consolidé")
st.caption("Données consolidées depuis 3 sources : CSV · API Taux de change · SQLite")

# ─── BOUTON DE RAFRAICHISSEMENT MANUEL ──────────────────────
col1, col2 = st.columns([1, 5])
with col1:
    if st.button("🔄 Rafraîchir les données"):
        # Relance le pipeline ETL complet pour mettre à jour DuckDB
        with st.spinner("Mise à jour des données en cours..."):
            run_etl()
        st.success("Données mises à jour !")

# ─── CONNEXION A DUCKDB ET CHARGEMENT DES DONNÉES ───────────
# On se connecte à la base DuckDB créée par l'ETL
conn = duckdb.connect("data/dataflow.duckdb")

# Lecture des 3 tables
df_ventes = conn.execute("SELECT * FROM ventes").df()
df_budgets = conn.execute("SELECT * FROM budgets").df()
df_taux = conn.execute("SELECT * FROM taux_change").df()

conn.close()

# ─── SECTION 1 : TAUX DE CHANGE ─────────────────────────────
st.divider()
st.subheader("💱 Taux de change en temps réel (base EUR)")
st.caption(f"Dernière mise à jour : {df_taux['date_maj'].values[0]}")

col1, col2, col3 = st.columns(3)
col1.metric("EUR / USD", f"{df_taux['EUR_USD'].values[0]:.4f}")
col2.metric("EUR / GBP", f"{df_taux['EUR_GBP'].values[0]:.4f}")
col3.metric("EUR / CHF", f"{df_taux['EUR_CHF'].values[0]:.4f}")

# ─── SECTION 2 : KPIs VENTES ────────────────────────────────
st.divider()
st.subheader("📊 KPIs Ventes")

# Calcul des indicateurs globaux
ca_total = df_ventes["total"].sum()
nb_transactions = len(df_ventes)
panier_moyen = ca_total / nb_transactions
meilleure_categorie = df_ventes.groupby("categorie")["total"].sum().idxmax()

col1, col2, col3, col4 = st.columns(4)
col1.metric("CA Total", f"{ca_total:,.0f} €")
col2.metric("Transactions", nb_transactions)
col3.metric("Panier moyen", f"{panier_moyen:,.0f} €")
col4.metric("Meilleure catégorie", meilleure_categorie)

# ─── SECTION 3 : VENTES PAR DÉPARTEMENT ─────────────────────
st.divider()
col1, col2 = st.columns(2)

with col1:
    st.subheader("Ventes par département")
    # Agrégation du CA par département
    ventes_dept = df_ventes.groupby("departement")["total"].sum().sort_values(ascending=False)
    st.bar_chart(ventes_dept)

with col2:
    st.subheader("Ventes par catégorie")
    # Agrégation du CA par catégorie de produit
    ventes_cat = df_ventes.groupby("categorie")["total"].sum().sort_values(ascending=False)
    st.bar_chart(ventes_cat)

# ─── SECTION 4 : ÉVOLUTION MENSUELLE ────────────────────────
st.divider()
st.subheader("📈 Évolution mensuelle du CA")

# Extraction du mois depuis la date et agrégation
df_ventes["mois"] = df_ventes["date"].dt.to_period("M").astype(str)
evolution = df_ventes.groupby("mois")["total"].sum()
st.line_chart(evolution)

# ─── SECTION 5 : BUDGETS PAR DÉPARTEMENT ────────────────────
st.divider()
st.subheader("💰 Suivi budgétaire par département")

# Mise en forme du tableau des budgets
df_budgets_display = df_budgets.copy()
df_budgets_display["budget_annuel"] = df_budgets_display["budget_annuel"].apply(lambda x: f"{x:,.0f} €")
df_budgets_display["budget_consomme"] = df_budgets_display["budget_consomme"].apply(lambda x: f"{x:,.0f} €")
df_budgets_display["budget_restant"] = df_budgets_display["budget_restant"].apply(lambda x: f"{x:,.0f} €")
df_budgets_display["taux_consommation"] = df_budgets_display["taux_consommation"].apply(lambda x: f"{x}%")
df_budgets_display.columns = ["Département", "Budget annuel", "Consommé", "Restant", "Taux (%)"]

st.dataframe(df_budgets_display, use_container_width=True)

# Alerte sur les départements qui dépassent 70% de consommation
st.divider()
df_alerte = df_budgets[df_budgets["taux_consommation"] > 70]
if not df_alerte.empty:
    st.warning(f"⚠️ **Alerte budget** — {len(df_alerte)} département(s) ont consommé plus de 70% de leur budget :")
    for _, row in df_alerte.iterrows():
        st.write(f"- **{row['departement']}** : {row['taux_consommation']}% consommé")
else:
    st.success("Tous les départements sont dans les limites budgétaires.")

# ─── SECTION 6 : DONNÉES BRUTES ─────────────────────────────
st.divider()
with st.expander("📋 Voir les données brutes des ventes"):
    # Section dépliable — visible uniquement si l'utilisateur clique
    st.dataframe(df_ventes, use_container_width=True)
