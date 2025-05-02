import pandas as pd
from pathlib import Path
from prefect import flow, task


# ----------------------------- Configuration -----------------------------

# Placeholder base path (update for production)
DATA_BASE_PATH = Path("<<YOUR_NETWORK_OR_LOCAL_DATA_PATH>>")

# Folder/file patterns (per country F01, F02, etc.)
CSV_FOLDER_PATTERN = "CSV/F0{}/"
EXPORT_REPORT_FILE = "Data/export_report-01-aa.csv"
EXPORT_JG_FILE = "Data/export-01-aa-JG.csv"
WARENGRUPPE_FILE = "Data/Warengruppe.csv"
OUTPUT_FILE_PATTERN = "rechnung/rechnung_F0{}.csv"

# ------------------------------- ETL Tasks -------------------------------

@task
def load_csv(filepath, sep=";", encoding="cp850"):
    """Load a CSV file safely with low_memory disabled."""
    return pd.read_csv(filepath, sep=sep, encoding=encoding, low_memory=False)


def preprocess_v2ad1056(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and filter V2AD1056 data."""
    df['AUF_ANLAGE'] = pd.to_datetime(df['AUF_ANLAGE'], format='%Y-%m-%d', errors='coerce')
    df['RECH_NR'] = df['RECH_NR'].astype(str).str.replace('.0', '').str.rjust(12, '0')
    df['NUMMER'] = df['VERWEIS'].str[2:12].astype(str).str.replace('.0', '').str.zfill(10)
    return df[df['AUF_ANLAGE'] >= '2023-01-01']


def preprocess_v2ad1156(df: pd.DataFrame, export_data: pd.DataFrame) -> pd.DataFrame:
    """Clean V2AD1156 data and enrich with thumbnail info."""
    df['RECHNUNG'] = df['RECHNUNG'].astype(str).str.replace('.0', '').str.rjust(12, '0')
    df = pd.merge(df, export_data, on='ART_NR', how='left')
    df['Thumbnail'] = df['Thumbnail'].astype(str)
    df.loc[df['PREIS'] < 0, 'MENGE'] *= -1
    return df


def load_export_data() -> pd.DataFrame:
    """Load and combine export report and JG data."""
    hg = pd.read_csv(DATA_BASE_PATH / EXPORT_REPORT_FILE, usecols=['ProdIndex', 'Thumbnail'],
                     sep='\t', encoding='latin-1', on_bad_lines='skip')
    jg = pd.read_csv(DATA_BASE_PATH / EXPORT_JG_FILE, usecols=['ProdIndex', 'Thumbnail'],
                     sep='\t', encoding='latin-1', on_bad_lines='skip')
    data = pd.concat([hg, jg]).drop_duplicates(subset='ProdIndex')
    return data.rename(columns={'ProdIndex': 'ART_NR'})


def merge_data(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """Inner join of the two input DataFrames on invoice numbers."""
    return pd.merge(df1, df2, left_on='RECH_NR', right_on='RECHNUNG', how='inner')


def load_warengruppe_mapping() -> dict:
    """Load Warengruppe CSV and return as dictionary mapping."""
    wg = pd.read_csv(DATA_BASE_PATH / WARENGRUPPE_FILE, sep=';', encoding='latin-1')
    wg['WAREN_GRP'] = wg['WAREN_GRP'].astype(str).str.replace('.0', '')
    return dict(zip(wg['WAREN_GRP'], wg['WAREN_GRNA']))


def enrich_and_save(df: pd.DataFrame, v2ar1001: pd.DataFrame, land: int):
    """Add Warengruppe info and export final CSV."""
    # Join with ART_NR to get WG
    v2ar1001 = v2ar1001.rename(columns={'NUMMER': 'ART_NR', 'WARENGR': 'WG'})[['ART_NR', 'WG']].drop_duplicates()
    df = pd.merge(df, v2ar1001, on='ART_NR', how='left')

    # Map WG to WG_NAME
    wg_map = load_warengruppe_mapping()
    df['WG'] = df['WG'].astype(str).str.replace('.0', '')
    df['WG_NAME'] = df['WG'].map(wg_map)

    # Final cleaning
    df['NUMMER'] = df['NUMMER'].astype(str).str.replace('.0', '').str.zfill(10)
    df['AUFTRAG_NR'] = df['AUFTRAG_NR'].astype(str).str.replace('.0', '').str.zfill(9)

    selected_cols = [
        'VERWEIS', 'AUFTRAG_NR', 'HERKUNFT', 'TYP', 'DATUM', 'MEDIACODE', 'NUMMER',
        'AUF_ANLAGE', 'RECHNUNG', 'ART_NR', 'GROESSE', 'FARBE', 'MENGE', 'PREIS',
        'MWST', 'WG', 'WG_NAME', 'EK', 'BEZEICHNG', 'RETOUREGRD', 'RETOUREART',
        'RECH_ART', 'Thumbnail'
    ]

    df[selected_cols].drop_duplicates().to_csv(
        DATA_BASE_PATH / OUTPUT_FILE_PATTERN.format(land),
        sep=';', encoding='cp850', index=False
    )

# ----------------------------- Main Flow -----------------------------

@flow
def user_rechnung():
    """Main ETL flow for Rechnungen (Invoices) by region."""
    export_data = load_export_data()

    for i in range(1, 5):
        folder = DATA_BASE_PATH / CSV_FOLDER_PATTERN.format(i)

        v2ad1056 = load_csv(folder / "V2AD1056.csv")
        v2ad1156 = load_csv(folder / "V2AD1156.csv")
        v2ar1001 = load_csv(DATA_BASE_PATH / "CSV/F01/V2AR1001.csv")

        v2ad1056 = preprocess_v2ad1056(v2ad1056)
        v2ad1156 = preprocess_v2ad1156(v2ad1156, export_data)
        merged_data = merge_data(v2ad1056, v2ad1156)

        enrich_and_save(merged_data, v2ar1001, i)

# ---------------------------- Script Entry -----------------------------

if __name__ == "__main__":
    user_rechnung()
