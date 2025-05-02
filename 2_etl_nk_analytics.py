import pandas as pd
import dask.dataframe as dd
from datetime import datetime
from prefect import flow
from pathlib import Path

# --------------------------- Configuration ---------------------------

BASE_PATH = Path("<<YOUR_BASE_PATH>>")
EXPORT_PATHS = [
    Path("<<YOUR_EXPORT_PATH_1>>/NK_data_all_lands.xlsx")
]

ANREDE_MAPPING = {
    '1': 'Herrn', '2': 'Frau', '3': 'Frau/Herr', '4': 'Firma', '5': 'Leer(Firmenadresse)', 
    '6': 'Fräulein', '7': 'Familie', 'X': 'Divers'
}

RETOURGRD_MAPPING = {
    "01": "Falscher Artikel", "02": "Lieferung zu spät", "03": "Artikel doppelt geliefert",
    "04": "Artikel gefällt nicht", "05": "Anders als abgebildet", "06": "Artikel ist defekt",
    "07": "Lieferung verschmutzt/beschädigt", "08": "Preis-/Leistungsverhältnis", "09": "Artikel zur Auswahl",
    "10": "Qualität/Mat. gefällt nicht", "11": "Farbe gefällt nicht", "12": "Zu klein/zu eng",
    "13": "Zu groß/zu weit", "14": "Zu kurz", "15": "Zu lang", "16": "Form/Schnitt gefällt nicht",
    "17": "Fehler bei Erfassung", "18": "Aufträge zusammengefasst", "19": "Preisnachlass vereinbart",
    "20": "Preisnachlass wegen defekt", "21": "Annahme verweigert", "22": "Nicht zugestellt/abgeholt",
    "23": "Ware ist gebraucht/benutzt", "24": "Wurde nicht bestellt", "25": "Postweg verloren gegangen",
    "26": "Post beschädigt", "27": "Fehler bei der Lieferung", "28": "Kühlkette unterbrochen",
    "29": "Keine Angabe vom Kunden", "30": "Nachbearbeitung", "31": "MHD abgelaufen", 
    "98": "Kulanz", "99": "Lager Storno"
}

# --------------------------- Helper Functions ---------------------------

def get_half_year_starts():
    today = datetime.today()
    if today.month <= 6:
        return (
            f"{today.year}-01-01", f"{today.year-1}-07-01", f"{today.year-1}-01-01"
        )
    return (
        f"{today.year}-07-01", f"{today.year}-01-01", f"{today.year-1}-07-01"
    )


def load_csv(path):
    return pd.read_csv(path, sep=";", encoding="cp850", on_bad_lines="skip", low_memory=False)


def load_dask_csv(path, usecols):
    return dd.read_csv(path, sep=";", encoding="cp850", on_bad_lines="skip", dtype="object", usecols=usecols)


def process_anrede(value):
    return ANREDE_MAPPING.get(str(value).replace("0", "").replace(".0", ""), "")


def process_retouren(value):
    return RETOURGRD_MAPPING.get(str(value).replace(".0", ""), "")


def preprocess_and_merge(adresse, stat, rechnung, land, last_hj, this_hj, two_hj):
    adresse = adresse[adresse["LKZ"] == land_code(land)].drop_duplicates("NUMMER", keep="last")
    stat = stat.drop_duplicates("NUMMER", keep="last")

    adresse["NUMMER"] = adresse["NUMMER"].astype(str)
    stat["NUMMER"] = stat["NUMMER"].astype(str)
    rechnung["NUMMER"] = rechnung["NUMMER"].astype(str).str.zfill(10)
    rechnung["Thumbnail"] = rechnung["Thumbnail"].astype(str).fillna("").str.replace("https://", "")

    merged = dd.merge(adresse, stat, on="NUMMER", how="inner").compute()
    merged = pd.merge(merged, rechnung, on="NUMMER", how="inner")

    merged["ERSTKAUF"] = pd.to_datetime(merged["ERSTKAUF"], errors="coerce")
    merged["ANREDE"] = merged["ANREDE"].apply(process_anrede)
    merged["RETOUREGRD"] = merged["RETOUREGRD"].apply(process_retouren)
    merged = merged[merged["ERSTKAUF"] >= two_hj]

    today = datetime.today().strftime('%Y-%m-%d')
    merged["NK_STATUS"] = ""
    merged["HJ_DATES"] = ""

    merged.loc[merged["ERSTKAUF"] >= this_hj, "NK_STATUS"] = "Current NK"
    merged.loc[(merged["ERSTKAUF"] >= last_hj) & (merged["ERSTKAUF"] < this_hj), "NK_STATUS"] = "Previous NK"
    merged.loc[(merged["ERSTKAUF"] >= two_hj) & (merged["ERSTKAUF"] < last_hj), "NK_STATUS"] = "Two Half-Years Ago NK"

    merged.loc[merged["ERSTKAUF"] >= this_hj, "HJ_DATES"] = f"NK_ab_{this_hj}_bis_{today}"
    merged.loc[(merged["ERSTKAUF"] >= last_hj) & (merged["ERSTKAUF"] < this_hj), "HJ_DATES"] = f"NK_ab_{last_hj}_bis_{this_hj}"
    merged.loc[(merged["ERSTKAUF"] >= two_hj) & (merged["ERSTKAUF"] < last_hj), "HJ_DATES"] = f"NK_ab_{two_hj}_bis_{last_hj}"

    for col in ["MENGE", "PREIS", "MWST"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")

    for col in ["GEBURT", "DATUM", "ERSTKAUF", "KAUFDATUM"]:
        merged[col] = pd.to_datetime(merged[col], errors="coerce").dt.date

    return merged


def land_code(land_id):
    return {"F01": "D", "F02": "F", "F03": "A", "F04": "CH"}.get(land_id, "")


def assign_age(df):
    today = datetime.now()
    df["AGE"] = df["GEBURT"].apply(lambda x: today.year - x.year - ((today.month, today.day) < (x.month, x.day)))
    df["AGE_GROUP"] = pd.cut(
        df["AGE"],
        bins=[0, 18, 30, 50, 65, 150],
        labels=["0-18", "19-30", "31-50", "51-65", "65+"],
        right=True
    )
    return df


def assign_sources(df):
    df["SOURCE"] = ""
    df["ON-OFF"] = ""

    # This part can be modularized into pattern match groups for scalability,
    # but we keep it inline here for clarity.
    df.loc[df["QUELLE"].str[3:] == "921am", "SOURCE"] = "Amazon"
    df.loc[df["QUELLE"].str[3:6] == "929", "SOURCE"] = "AWIN"
    # ... Task-specific function, cannot be adopted
    df["SOURCE"] = df["SOURCE"].replace("", "Altcode")

    # ON/OFF assignment
    df["ON-OFF"] = df["SOURCE"].map(lambda s: "Online" if s in ONLINE_SOURCES else ("Offline" if s in OFFLINE_SOURCES else "Altcode"))
    return df


ONLINE_SOURCES = {
    "Amazon", "AWIN", "Blätterkatalog", "Corporate Benefits", "Genussmagazin", "Google Shopping",
    "Internet Import", "Inventur Trost", "Lionshome", "Newsletter", "Newsletter Angebot", "Newsletter Rezept",
    "Newsletter Thema", "Otto", "Google SEA", "SEA Brand", "SEA Non-Brand", "SEO", "SEO Brand", "SEO Non-Brand",
    "Social Media", "Pinterest", "Instagram", "Facebook", "Sovendus"
}

OFFLINE_SOURCES = {
    "Fremdadressen", "Katalog und Karte", "Beilage", "Geburtstagskarte", "Kataloganforderung",
    "Freundschaftswerbung", "Mailing", "Blackweek"
}

# ----------------------------- Flow -----------------------------

@flow
def etl_process():
    this_hj, last_hj, two_hj = get_half_year_starts()
    results = []

    for land in ["F01", "F02", "F03", "F04"]:
        rechnung = load_csv(BASE_PATH / f"rechnung/rechnung_{land}.csv")
        adresse = load_dask_csv(BASE_PATH / f"CSV/{land}/V2AD1001.csv", ["NUMMER", "ANREDE", "TITEL", "VORNAME", "NAME", "QUELLE", "GEBURT", "LKZ", "PLZ", "ORT"])
        stat = load_dask_csv(BASE_PATH / f"CSV/{land}/V2AD1005.csv", ["NUMMER", "PREIS_GRP", "ANZ_AUF_G", "DM_AUF_G", "KAUFDATUM", "ERSTKAUF"])
        
        merged = preprocess_and_merge(adresse, stat, rechnung, land, last_hj, this_hj, two_hj)
        merged = assign_age(merged)
        merged = assign_sources(merged)
        results.append(merged)

    final_df = pd.concat(results)
    for path in EXPORT_PATHS:
        final_df.to_excel(path, index=False, engine="xlsxwriter")

# ----------------------------- Run -----------------------------

if __name__ == "__main__":
    etl_process()
