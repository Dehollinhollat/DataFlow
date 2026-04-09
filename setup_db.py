import sqlite3
import os

os.makedirs("data", exist_ok=True)

conn = sqlite3.connect("data/dataflow.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS budgets (
    departement TEXT PRIMARY KEY,
    budget_annuel REAL,
    budget_consomme REAL
)
""")

budgets = [
    ("Commercial", 50000, 28000),
    ("Finance", 30000, 18500),
    ("RH", 25000, 12000),
    ("Marketing", 35000, 22000),
    ("Direction", 40000, 15000),
    ("Informatique", 60000, 35000),
]

cursor.executemany("INSERT OR REPLACE INTO budgets VALUES (?, ?, ?)", budgets)
conn.commit()
conn.close()

print("Base de données créée avec succès")
