import csv # pour lire et écrire dans un csv
import json # pour écrire du json
import os # pour travailler avec le système d'exploitation
import urllib.request #pour téléchergar un fichier via une URL

RAW_URL = os.getenv("RAW_URL") # URL du fichier a télécharger
RAW_PATH = os.getenv("RAW_PATH", "/data/raw.csv") # chemin où stocker le fichier par défaut /data/raw.csv
CLEANED_PATH = os.getenv("CLEANED_PATH", "/data/cleaned.csv") #Fichier de sortie après traitement
JSONL_PATH = os.getenv("JSONL_PATH", "/data/messages.jsonl") #Sortie JSONL

def norm(s: str) -> str:
    """
    Docstring pour norm
    Normalise un nom de colonne :
    enlève espaces autour
    passe en minuscules
    remplace espaces par _
    Ex: "Nom Station" → "nom_station"
    """
    return s.strip().lower().replace(" ", "_")

def to_int(x):
    """
    Convertit une valeur en entier "robuste" :
    None / vide → None
    accepte des floats en texte ("12.0") → 12
    si ça ne marche pas → None
    """
    if x is None:
        return None
    x = str(x).strip()
    if x == "":
        return None
    try:
        return int(float(x))
    except:
        return None

def to_bool01_fr(x):
    """
    Convertit des valeurs françaises possibles en 0/1:
    "oui", "true", "1", "vrai" → 1
    "non", "false", "0", "faux" → 0
    vide / inconnu → None
    """
    if x is None:
        return None
    s = str(x).strip().lower()
    if s == "":
        return None
    if s in ("oui", "true", "1", "vrai"):
        return 1
    if s in ("non", "false", "0", "faux"):
        return 0
    return None

def download_if_needed():
    """
    Si RAW_PATH existe et n’est pas vide → on ne retélécharge pas
    Sinon, il faut RAW_URL → sinon erreur
    Télécharge le CSV depuis l’URL vers RAW_PATH
    """
    if os.path.exists(RAW_PATH) and os.path.getsize(RAW_PATH) > 0:
        print(f"[cleaner] raw.csv déjà présent: {RAW_PATH}")
        return
    if not RAW_URL:
        raise RuntimeError("RAW_URL manquant (variable d'environnement).")
    print(f"[cleaner] téléchargement: {RAW_URL}")
    urllib.request.urlretrieve(RAW_URL, RAW_PATH)
    print(f"[cleaner] téléchargé -> {RAW_PATH}")

def main():
    #Prépare le dossier et s’assure d’avoir le CSV brut
    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)
    download_if_needed()
    # Ouvre le CSV brut avec les bons paramètres
    # delimiter=";" : CSV français classique séparé par point-virgule
    # IMPORTANT: encoding utf-8-sig enlève le BOM (\ufeff) qui casse parfois le premier header
    with open(RAW_PATH, newline="", encoding="utf-8-sig") as f:
        # DictReader lit chaque ligne sous forme de dictionnaire {colonne: valeur}
        reader = csv.DictReader(f, delimiter=";")
        #Détecte les colonnes et construit un mapping normalisé
        # Ex: "Nom station" → "nom_station"
        raw_fields = reader.fieldnames or []
        print("[cleaner] Colonnes détectées:", raw_fields)
        # mapping original -> normalisé
        mapping = {c: norm(c) for c in raw_fields if c is not None}
        cleaned_rows = []
        #Parcourt les lignes, nettoie les clés/valeurs
        for row in reader:
            r = {}
            # row peut contenir une clé None si parsing foire ou colonnes en trop:
            for k, v in row.items():
                if k is None:
                    # valeurs "en trop" (liste) -> on ignore
                    continue
                r[mapping[k]] = v.strip() if isinstance(v, str) else v
            # Filtre minimal : ignore les lignes incomplètes
            station_code = r.get("identifiant_station")
            station_name = r.get("nom_station")
            due_date = r.get("actualisation_de_la_donnée") or r.get("actualisation_de_la_donnee")
            if not station_code or not due_date:
                continue
            # Construit une ligne “propre” (schéma standard)
            #Points importants :
            # Il gère les variations d’accents dans les headers :
            # "capacité_de_la_station" ou "capacite_de_la_station"
            # "vélos_électriques_disponibles" ou "velos_electriques_disponibles"
            # Il convertit les champs numériques avec to_int
            # Il convertit certains champs booléens avec to_bool01_fr
            # due_date est gardée en string (le commentaire dit que le consommateur la parsera plus tard)
            # Ajoute des champs bonus (commune, code_insee, geo)
            out = {
                "station_code": str(station_code).strip(),
                "name": station_name,
                "is_installed": to_bool01_fr(r.get("station_en_fonctionnement")),
                "capacity": to_int(r.get("capacité_de_la_station") or r.get("capacite_de_la_station")),
                "numdocksavailable": to_int(r.get("nombre_bornettes_libres")),
                "numbikesavailable": to_int(r.get("nombre_total_vélos_disponibles") or r.get("nombre_total_velos_disponibles")),
                "mechanical": to_int(r.get("vélos_mécaniques_disponibles") or r.get("velos_mecaniques_disponibles")),
                "ebike": to_int(r.get("vélos_électriques_disponibles") or r.get("velos_electriques_disponibles")),
                "is_returning": to_bool01_fr(r.get("retour_vélib_possible") or r.get("retour_velib_possible")),
                # due_date (on garde la string, le consumer la parse)
                "due_date": str(due_date).strip(),
                "commune": r.get("nom_communes_équipées") or r.get("nom_communes_equipees"),
                "code_insee": r.get("code_insee_communes_équipées") or r.get("code_insee_communes_equipees"),
                "geo": r.get("coordonnées_géographiques") or r.get("coordonnees_geographiques"),
            }
            #Tout ça est ajouté à cleaned_rows
            cleaned_rows.append(out)
    #Vérifie qu’on a bien des données
    if not cleaned_rows:
        raise RuntimeError("Aucune ligne nettoyée (vérifie les noms de colonnes / parsing).")
    #Écrit le CSV nettoyé
    # Note : il prend les colonnes à partir de la première ligne (cleaned_rows[0].keys()).
    # Si certaines lignes ont des clés différentes (normalement non), ça peut poser problème.
    fields = list(cleaned_rows[0].keys())
    with open(CLEANED_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(cleaned_rows)
    #Écrit le JSONL
    # JSONL = un JSON par ligne
    #ensure_ascii=False conserve les accents
    with open(JSONL_PATH, "w", encoding="utf-8") as f:
        for r in cleaned_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[cleaner] OK: {len(cleaned_rows)} lignes")
    print(f"[cleaner] -> {CLEANED_PATH}")
    print(f"[cleaner] -> {JSONL_PATH}")

if __name__ == "__main__":
    main()
