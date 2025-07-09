import pandas as pd
import requests

# Konfiguration
EXCEL_PATH = "rolloutliste.xlsx"
TOKEN_URL = "https://x.x.com/auth/authentication-endpoint/authenticate/token?TENANTID=x"
LOGIN_CREDENTIALS = {"login": "x", "password": "x"}
CREATE_PRINTER_URL = "https://x.x.com/rest/ems/bulk"
CHECK_URL_TEMPLATE = "https://x.x.com/rest/ems/Device?layout=SerialNumber&filter=SerialNumber = '{serial}'"
LOCATION_URL_TEMPLATE = "https://x.x.com/rest/ems/Location?layout=FullName,Id&filter=FullName = '{fullname}'"
CHECK_LOCATION_TEMPLATE = "https://x.x.com/rest/ems/Device?layout=SerialNumber,LocatedAtLocation&filter=SerialNumber = '{serial}'"

# Authentifizierung
def authenticate():
    try:
        response = requests.post(TOKEN_URL, json=LOGIN_CREDENTIALS)
        response.raise_for_status()
        return {"LWSSO_COOKIE_KEY": response.text.strip()}
    except Exception as e:
        print(f"Fehler beim Abrufen des Tokens: {e}")
        exit()

# Excel einlesen
def read_excel(path):
    df = pd.read_excel(path, header=2, usecols="A:P")
    df.columns = df.columns.str.strip()
    return df

# Standort-String umwandeln
def map_location(standort):
    try:
        parts = standort.split('-')
        gebaeude = parts[1].replace("Geb", "").strip()
        stockwerk = parts[2].strip()
        raum = parts[3].replace("Raum", "").strip()
        return f"Region:EU;Country:DE;City:OS;Office:CUOS;Building:{gebaeude};Floor:{stockwerk};Room:{raum}"
    except Exception as e:
        print(f"Fehler beim Mapping des Standorts '{standort}': {e}")
        return None

# Location-ID abrufen
def get_location_id(mapped_location, cookie):
    url = LOCATION_URL_TEMPLATE.format(fullname=mapped_location)
    response = requests.get(url, cookies=cookie)
    response.raise_for_status()
    data = response.json()
    if data.get("entities"):
        return data["entities"][0]["properties"]["Id"]
    print(f"Keine Location-ID gefunden für: {mapped_location}")
    return None

# Prüfen, ob Gerät existiert
def device_exists(serial, cookie):
    url = CHECK_URL_TEMPLATE.format(serial=serial)
    response = requests.get(url, cookies=cookie)
    response.raise_for_status()
    return response.json().get("entities")

# Geräte-ID abrufen
def get_device_id(serial, cookie):
    url = CHECK_LOCATION_TEMPLATE.format(serial=serial)
    response = requests.get(url, cookies=cookie)
    response.raise_for_status()
    data = response.json()
    if data.get("entities"):
        return data["entities"][0]["properties"]["Id"]
    return None

# Payload für API-Aufruf erstellen
def build_payload(daten, location_id, operation, device_id=None):
    properties = {
        "AssetTag": daten["geraete_nr"],
        "SubType": "NetworkPrinter",
        "Model": f"Kyocera {daten['neues_system']}",
        "SerialNumber": daten["seriennummer"],
        "DisplayLabel": daten["hostname"],
        "ShortDescription": f"IP-Adresse: {daten['ip_adresse']} MAC: {daten['mac']}",
        "HostName": daten["hostname"],
        "LocatedAtLocation": location_id,
        "InvLocation_c": daten["standort"]
    }
    if device_id:
        properties["Id"] = device_id

    return {
        "entities": [{"entity_type": "Device", "properties": properties}],
        "operation": operation
    }

# Einzelnen Drucker verarbeiten
def process_printer(row, cookie):
    if row.isnull().all():
        return

    daten = {
        "nummer": row["Nr"],
        "neues_system": row["Neues System"],
        "seriennummer": row["Ser-Nr"],
        "adf": row["ADF"],
        "kassette": row["Kassette"],
        "ablade": row["Ablade"],
        "gebaeude": row["Geb."],
        "raum": row["Raum"],
        "standort": row["Standort"],
        "hostname": row["Hostname"],
        "ip_adresse": row["IP Adresse"],
        "mac": row["MAC"],
        "geraete_nr": row["Geräte-NR"],
        "auslieferung": row["Auslief."],
        "altgeraet": row["Altgerät"],
        "alt_seriennummer": row["Seriennumer"],
    }

    mapped_location = map_location(daten["standort"])
    if not mapped_location:
        return

    location_id = get_location_id(mapped_location, cookie)
    if not location_id:
        return

    try:
        if device_exists(daten["seriennummer"], cookie):
            device_id = get_device_id(daten["seriennummer"], cookie)
            if device_id:
                payload = build_payload(daten, location_id, "UPDATE", device_id)
                print(f"Aktualisiere Drucker '{daten['hostname']}'...")
            else:
                print(f"Kein Standort für '{daten['hostname']}' gefunden.")
                return
        else:
            payload = build_payload(daten, location_id, "CREATE")
            print(f"Erstelle neuen Drucker '{daten['hostname']}'...")

        response = requests.post(CREATE_PRINTER_URL, cookies=cookie, json=payload)
        response.raise_for_status()
        print(f"Drucker '{daten['hostname']}' erfolgreich verarbeitet.")
    except Exception as e:
        print(f"Fehler bei Drucker '{daten['hostname']}': {e}")

# Hauptfunktion
def main():
    cookie = authenticate()
    df = read_excel(EXCEL_PATH)
    for _, row in df.iterrows():
        process_printer(row, cookie)

if __name__ == "__main__":
    main()
