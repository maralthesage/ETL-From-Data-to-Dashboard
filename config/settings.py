"""
Configuration settings for the ETL pipeline.
This file contains all configurable parameters and paths.
"""

import os
from pathlib import Path
from typing import Dict, List

# Base paths - replace with your actual data paths
BASE_DATA_PATH = os.getenv("DATA_PATH", "/path/to/your/data")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/path/to/your/output")

# Country/Region mappings
COUNTRY_MAPPING = {
    'F01': 'DE',  # Germany
    'F02': 'FR',  # France  
    'F03': 'AT',  # Austria
    'F04': 'CH'   # Switzerland
}

# Supported countries/regions
SUPPORTED_COUNTRIES = list(COUNTRY_MAPPING.keys())

# File encoding and separator settings
ENCODING = "cp850"
SEPARATOR = ";"

# CSV reading parameters
CSV_READ_KWARGS = {
    "sep": SEPARATOR,
    "encoding": ENCODING,
    "low_memory": False,
    "dtype_backend": "numpy_nullable"
}

# Dask configuration
DASK_CONFIG = {
    "blocksize": "64MB",
    "memory_limit": "6GB",
    "n_workers": max(os.cpu_count() - 1, 1),
    "threads_per_worker": 1
}

# RFM Analysis Configuration
RFM_CONFIG = {
    "MONETARY_BINS": [0, 100, 200, 500, 1000, float("inf")],
    "MONETARY_LABELS": [1, 2, 3, 4, 5],
    "FREQ_BINS": [0, 1, 3, 6, 15, float("inf")],
    "FREQ_LABELS": [1, 2, 3, 4, 5]
}

# Customer segmentation labels
CUSTOMER_SEGMENTS = [
    "01-Champions", "02-Treue Kunden", "03-Nicht zu verlieren", 
    "04-Potenziell loyale Kunden", "05-Brauchen Aufmerksamkeit", 
    "06-Gefährdete Kunden", "07-Neukunden", "08-Reaktivierte Kunden",
    "09-Vielversprechende Kunden", "10-Abwandernde Kunden", 
    "11-Schlafende Kunden", "12-Verlorene Kunden", 
    "13-Interessenten", "Nicht klassifiziert"
]

# Data source mappings
SOURCE_MAPPINGS = {
    'amazon': 'Amazon',
    'awin': 'AWIN', 
    'google': 'Google',
    'newsletter': 'Newsletter',
    'social_media': 'Social Media',
    'offline': 'Offline'
}

# Age group definitions
AGE_GROUPS = {
    '0-18': (0, 18),
    '19-30': (19, 30),
    '31-50': (31, 50),
    '51-65': (51, 65),
    '65+': (65, 999)
}

# File paths (to be configured per environment)
FILE_PATHS = {
    'customer_addresses': 'customer_addresses.csv',
    'customer_transactions': 'customer_transactions.csv', 
    'customer_emails': 'customer_emails.csv',
    'advertising_data': 'advertising_data.csv',
    'product_catalog': 'product_catalog.csv',
    'warehouse_groups': 'warehouse_groups.csv',
    'postal_codes': 'postal_codes_germany.xlsx',
    'email_preferences': 'email_customer_mapping.xlsx',
    'customer_segments': 'customer_segments.xlsx'
}

# Output file naming
OUTPUT_FILES = {
    'customer_analysis': 'customer_analysis_{country}.csv',
    'rfm_segments': 'rfm_segments_{country}.csv',
    'customer_groups': 'customer_groups_{country}.csv'
}
