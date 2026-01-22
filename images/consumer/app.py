import json
import os
import time
from datetime import datetime

from kafka import KafkaConsumer # Pour lire un topic kafka
import mysql.connector


# Configuration Kafka
TOPIC = os.getenv("KAFKA_TOPIC", "velib-stations") # Nom du topic
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092") #Adresse du broker 
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "velib-consumer") # Groupe du consumer (permet la répartition + gestion d’offsets)

# PAramètres MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "velib")
MYSQL_USER = os.getenv("MYSQL_USER", "velib")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "velib")


def connect_mysql_with_retry(retries=30, delay=2):
    """
    Permet la connexion à MySQL avec retry car sous docker ça peut prendre du temps à être up
    il essaye jusqu'à 30 fois avec un délai de 2 secondes entre chaque tentatives
    """
    for i in range(retries):
        try:
            return mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DB,
            )
        except mysql.connector.Error as e:
            print(f"[mysql] connexion échouée ({i+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Impossible de se connecter à MySQL")


def ensure_table(conn):
    """
    S'assure que la table velib_status existe dans la base 
    Si elle n'existe pas elle est créé sinon aucune modif n'est faite

    # Colonnes d’identification
    # station_code : Identifiant unique de la station (ex: code Vélib).
    # station_name : Nom lisible de la station nullable car il peut manquer dans certaines sources/messages.
    # commune : Ville / arrondissement / commune. Nullable aussi.
    # 
    # Capacité et disponibilité des bornes
    # capacity : Nombre total de places/borne (capacité). Nullable.
    # docks_available : Nombre de bornes libres. Nullable.
    # 
    # Disponibilité vélos
    # bikes_available : Total vélos disponibles
    # bikes_mechanical : Vélos mécaniques disponibles.
    # bikes_ebike : Vélos électriques disponibles.
    # tous en int null car parfois on a pas de données ou illisible donc on met null au lieu de mauvais chiffres
    #
    # is_installed : station en service/installée
    # is_returning : retour possible (déposer un vélo)
    # code_insee : code insee de la commune
    # geo : Géolocalisation de la station

    """
    sql = """
    CREATE TABLE IF NOT EXISTS velib_status (
      station_code VARCHAR(32) NOT NULL,
      station_name VARCHAR(255) NULL,
      commune VARCHAR(255) NULL,

      capacity INT NULL,
      docks_available INT NULL,

      bikes_available INT NULL,
      bikes_mechanical INT NULL,
      bikes_ebike INT NULL,

      is_installed TINYINT NULL,
      is_returning TINYINT NULL,

      due_date DATETIME NOT NULL,
      code_insee VARCHAR(32)  NULL,
      geo VARCHAR(255) NULL,

      PRIMARY KEY (station_code, due_date)
    );
    """
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()


def to_int_or_none(x):
    """
    Convertit une valeur en int, sinon renvoie None.
    Les données provenant de Kafka/JSON peuvent être:
    - int, str, float, None, ou même des chaînes bizarres
    - On préfère insérer NULL en base plutôt qu'une valeur incorrecte.
    """

    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def parse_due_date(value):
    """
    Convertit une date venant du message en objet datetime (naïf, sans timezone).

    Les formats possibles selon la source:
    - ISO 8601: "2026-01-22T12:34:56+00:00" ou "...Z"
    - Format SQL-like: "2026-01-22 12:34:56" ou "2026-01-22 12:34"

    Notes importantes:
    - datetime.fromisoformat supporte +00:00 mais pas toujours "Z", donc on remplace "Z" par "+00:00"
    - MySQL DATETIME n'est pas timezone-aware: on enlève tzinfo si présent
    """
    if value is None:
        return None

    s = str(value).strip()
    s = s.replace("Z", "+00:00")

    # Essai ISO 8601
    try:
        dt = datetime.fromisoformat(s)
        # MySQL DATETIME n'aime pas le timezone aware -> on enlève tzinfo
        return dt.replace(tzinfo=None)
    except Exception:
        pass

    # Essai "YYYY-MM-DD HH:MM:SS"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    # Sinon, on abandonne
    return None


def insert_row(conn, msg):
    """
    Insère (ou met à jour) une ligne dans MySQL à partir d'un message Kafka (dict).

    msg:
    - dictionnaire issu de json.loads(record.value)

    Stratégie de mapping:
    - On tolère des variations de noms de colonnes (station_code vs stationcode vs stationCode)
      car les sources/exports peuvent évoluer.
    - On force certains champs indispensables:
      - station_code 
      - due_date 
      - code_insee
      - geo 
    - Les autres champs peuvent être NULL.

    ON DUPLICATE KEY UPDATE:
    - Si la clé primaire (station_code, due_date) existe déjà,
      on met à jour les colonnes avec les valeurs reçues.
    - Permet d'éviter les doublons, et de "compléter" des infos si un message arrive ensuite.
    """

    station_code = str(msg.get("station_code") or msg.get("stationcode") or msg.get("stationCode") or "").strip()
    if not station_code:
        raise ValueError("station_code manquant")

    due_date = parse_due_date(msg.get("due_date") or msg.get("duedate") or msg.get("last_reported"))
    if due_date is None:
        raise ValueError("due_date/duedate manquant ou illisible")

    station_name = msg.get("name") or msg.get("station_name")
    commune = msg.get("nom_arrondissement_communes") or msg.get("commune")

    capacity = to_int_or_none(msg.get("capacity"))
    docks_available = to_int_or_none(msg.get("numdocksavailable") or msg.get("docks_available"))

    bikes_available = to_int_or_none(msg.get("numbikesavailable") or msg.get("bikes_available"))
    bikes_mechanical = to_int_or_none(msg.get("mechanical") or msg.get("bikes_mechanical"))
    bikes_ebike = to_int_or_none(msg.get("ebike") or msg.get("bikes_ebike"))

    is_installed = to_int_or_none(msg.get("is_installed"))
    is_returning = to_int_or_none(msg.get("is_returning"))
    
    code_insee = str(msg.get("code_insee") or msg.get("codeinsee") or msg.get("code-insee") or "").strip()
    if not code_insee:
        raise ValueError("code_insee manquant")
    
    geo = str(msg.get("geo") or "").strip()
    if not geo:
        raise ValueError("geo manquant")

    sql = """
    INSERT INTO velib_status (
      station_code, station_name, commune,
      capacity, docks_available,
      bikes_available, bikes_mechanical, bikes_ebike,
      is_installed, is_returning,
      due_date, code_insee, geo
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      station_name=VALUES(station_name),
      commune=VALUES(commune),
      capacity=VALUES(capacity),
      docks_available=VALUES(docks_available),
      bikes_available=VALUES(bikes_available),
      bikes_mechanical=VALUES(bikes_mechanical),
      bikes_ebike=VALUES(bikes_ebike),
      is_installed=VALUES(is_installed),
      is_returning=VALUES(is_returning),
      code_insee=VALUES(code_insee),
      geo=VALUES(geo);
    """

    cur = conn.cursor()
    cur.execute(sql, (
        station_code, station_name, commune,
        capacity, docks_available,
        bikes_available, bikes_mechanical, bikes_ebike,
        is_installed, is_returning,
        due_date, code_insee, geo
    ))
    conn.commit()
    cur.close()


def main():
    """
    Point d'entrée:

    1) Connexion MySQL + création table si besoin
    2) Connexion KafkaConsumer
    3) Boucle infinie: consomme les messages, les parse, les insère
    4) Commit Kafka SEULEMENT si insertion OK (garantie au moins une fois / at-least-once)
    """
    print("[consumer] démarrage…")
    mysql_conn = connect_mysql_with_retry()
    ensure_table(mysql_conn)
    print("[consumer] MySQL OK, table prête.")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,   # on commit seulement après insertion MySQL
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8"),
    )
    print(f"[consumer] Kafka OK, écoute topic={TOPIC} group={GROUP_ID}")

    # Boucle de consommation:
    # `for record in consumer:` est bloquant: il attend des messages et itère à chaque message.
    for record in consumer:
        try:
            payload = json.loads(record.value)
            insert_row(mysql_conn, payload)
            # Commit Kafka:
            # -> on dit à Kafka "ce message (offset) est traité"
            # Important: commit après succès DB = cohérence (sinon on perd des messages)
            consumer.commit()  
            print(f"[consumer] inséré station={payload.get('station_code') or payload.get('stationcode')} offset={record.offset}")
        except Exception as e:
            # Important: on ne commit PAS, donc le message pourra être relu
            print(f"[consumer] ERREUR offset={record.offset}: {e} | valeur={record.value}")


if __name__ == "__main__":
    main()
