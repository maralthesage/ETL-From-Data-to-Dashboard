Here’s a professional and structured `README.md` tailored to your repository and the two-part Prefect-based ETL system:

---

## 📦 CSV ETL Pipeline for Customer Rechnungen

This repository implements a **two-part ETL pipeline** for processing and enriching customer invoice (Rechnungen) data. It is built with **Python**, uses **Pandas, Dask**, and leverages **Prefect** for robust **workflow orchestration**.

---

### ⚙️ Overview

The ETL process is split into two main stages:

---

### 🧩 Part 1 — `1_etl_rechnung_analytics.py`

**Goal:**  
Extract and prepare raw invoice data (from multiple country folders) and export clean Rechnungsdaten with thumbnails, product groups, and pricing.

**Key Steps:**
- ✅ Load raw CSVs (`V2AD1056.csv`, `V2AD1156.csv`, `V2AR1001.csv`)
- ✅ Enrich with thumbnails and Warengruppe names
- ✅ Normalize fields like article number, customer number, date
- ✅ Output clean Rechnungsdaten (`rechnung_F0X.csv`)

**Orchestrated by:**  
`@flow user_rechnung()` — runs once per land (F01–F04)

---

### 🧩 Part 2 — `2_etl_nk_analytics.py`

**Goal:**  
Merge Rechnungsdaten with customer and statistical data to extract insights about **new customer (NK)** segments.

**Key Steps:**
- ✅ Load Rechnungsdaten + `V2AD1001.csv` (addresses) + `V2AD1005.csv` (customer stats)
- ✅ Assign new customer status based on half-year logic
- ✅ Map return reasons and formal titles
- ✅ Enrich customers with:
  - 🎯 **Marketing source attribution** (QUELLE)
  - 📊 **Online/Offline classification**
  - 👤 **Age + Age group segmentation**
- ✅ Export master Excel sheet with all enriched customer rows

**Orchestrated by:**  
`@flow etl_process()` — iterates over all four lands (F01–F04)

---

### 🛠️ Technologies Used

| Tool       | Purpose                            |
|------------|------------------------------------|
| **Pandas** | Main data wrangling and exports    |
| **Dask**   | Efficient parallel CSV loading     |
| **Prefect**| Orchestrates the ETL workflows     |
| **Excel**  | Final NK dataset is exported here  |

---

### 🔁 Reusability & Extensibility

The ETL code is structured to be:

- **Modular**: each logical step is a clean Python function
- **Configurable**: file paths and mappings are externalized
- **Scalable**: ready to integrate more sources and deeper analytics
- **Safe**: sensitive paths removed for GitHub use

---

### 🚀 Running the Pipeline

Each ETL script can be run directly:

```bash
python 1_etl_rechnung_analytics.py
python 2_etl_nk_analytics.py
```

Or orchestrated via Prefect UI/CLI if integrated in a Prefect deployment.

---

### 📂 Folder Structure (example)

```
project/
├── 1_etl_rechnung_analytics.py     # Part 1: invoice data preparation
├── 2_etl_nk_analytics.py           # Part 2: customer enrichment
├── requirements.txt
└── README.md
```

