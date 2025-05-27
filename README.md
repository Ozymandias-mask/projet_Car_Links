import pandas as pd
import re
import requests
import os
import time

API_KEY = os.getenv("XXXXXXXXX")
INPUT_FILE = r"C:/Users/honor/Desktop/Ecole/Carrefour/Géniathon-Carrefour - BDE.xlsx"
OUTPUT_CSV = r"C:/Users/honor/Desktop/Ecole/Carrefour/holding_validation_perplexity_final.csv"
MAX_REQUESTS = 5

API_URL = "https://api.perplexity.ai/chat/completions"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

if not os.path.exists(INPUT_FILE):
    raise FileNotFoundError(f"❌ Fichier introuvable : {INPUT_FILE}")

df = pd.read_excel(INPUT_FILE)
df.columns = df.columns.str.strip().str.lower()

if not {"holding name", "brand name"}.issubset(df.columns):
    raise ValueError("❌ Les colonnes 'Holding Name' et 'Brand Name' sont manquantes dans le fichier.")

df = df[["holding name", "brand name"]]

df["vérification"] = ""
df["brand mise à jour"] = ""
df["certitude"] = ""

def check_affiliation_perplexity(holding, brand):
    messages = [
        {
            "role": "system",
            "content": (
                "Tu es un assistant expert en propriété de marques. "
                "Si aucune info fiable, fixes le degré de certitude <0,1. "
                "Calcules une estimation de la certitude de la réponse et notes-la de 0 à 1. "
                "Les sites comme Wikipédia ne sont pas considérés comme sûrs. "
                "BFM TV, les sites de l'État ou de la holding sont sûrs. "
                "Donne uniquement une réponse très courte, sans phrase, sous la forme suivante : 'oui (0.95)', 'non (0.3)', ou 'pas d'informations (0.05)'."
            )
        },
        {
            "role": "user",
            "content": f"Est-ce que la marque {brand} appartient toujours à {holding} en 2025 ?"
        }
    ]

    payload = {
        "model": "sonar-pro",
        "messages": messages
    }

    response = requests.post(API_URL, headers=HEADERS, json=payload)
    response.raise_for_status()

    result = response.json()
    print("🟦 Réponse brute API :", result)
    return result["choices"][0]["message"]["content"]

requests_done = 0

for idx, row in df.iterrows():
    if requests_done >= MAX_REQUESTS:
        df.at[idx, "vérification"] = "Non vérifié (limite atteinte)"
        continue

    holding = row["holding name"]
    brand = row["brand name"]

    print(f"\n🔎 Vérification {requests_done + 1}/{MAX_REQUESTS} : {holding} → {brand}")

    try:
        result = check_affiliation_perplexity(holding, brand)
        print("✅ Réponse API :", result)
    except Exception as e:
        print("❌ Erreur API :", e)
        df.at[idx, "vérification"] = "Erreur API"
        requests_done += 1
        continue

    res_lower = result.lower()
    requests_done += 1
    time.sleep(2)

    # === Certitude ===
    match_certitude = re.search(r"\(\s*([0-9.]+)\s*\)", res_lower)
    certitude = float(match_certitude.group(1)) if match_certitude else 0.0
    df.at[idx, "certitude"] = round(certitude, 2) if 0 <= certitude <= 1 else 0.0

    # === Vérification & Brand update ===
    if res_lower.startswith("oui"):
        df.at[idx, "vérification"] = "Correct"
        df.at[idx, "brand mise à jour"] = holding
    elif res_lower.startswith("non"):
        df.at[idx, "vérification"] = "Incorrect"
        match = re.search(r"appartient à ([^.()]+)", res_lower)
        df.at[idx, "brand mise à jour"] = match.group(1).strip() if match else ""
    elif res_lower.startswith("pas d'informations"):
        df.at[idx, "vérification"] = "Inconnu"
        df.at[idx, "brand mise à jour"] = ""
    else:
        df.at[idx, "vérification"] = "Inconnu"
        df.at[idx, "brand mise à jour"] = ""

    # Sauvegarde intermédiaire toutes les 5 lignes
    if requests_done % 5 == 0:
        df.to_csv(OUTPUT_CSV, index=False, sep=';', encoding="utf-8-sig")

df.to_csv(OUTPUT_CSV, index=False, sep=';', encoding="utf-8-sig")

print("\n✅ Analyse terminée. Résultats enregistrés dans :", OUTPUT_CSV)
print("📁 Chemin complet :", os.path.abspath(OUTPUT_CSV))
