"""
Data processing utilities for the ETL pipeline.
Contains reusable functions for data cleaning, transformation, and validation.
"""

import pandas as pd
import numpy as np
from typing import Union, Dict, Any
from datetime import datetime, date
from dateutil.relativedelta import relativedelta


def pad_customer_id(df: pd.DataFrame, column_name: str = "customer_id", width: int = 10) -> pd.DataFrame:
    """
    Pad customer ID column with leading zeros and clean up formatting.
    
    Args:
        df: DataFrame containing the ID column
        column_name: Name of the ID column to process
        width: Target width for zero-padding
        
    Returns:
        DataFrame with processed ID column
    """
    df = df.copy()
    df[column_name] = df[column_name].astype("string")
    
    # Remove ".0" suffix if present
    df[column_name] = df[column_name].str.replace(".0", "", regex=False)
    
    # Pad with leading zeros
    df[column_name] = df[column_name].str.zfill(width)
    
    return df


def calculate_age_groups(df: pd.DataFrame, birth_date_col: str = "birth_date") -> pd.DataFrame:
    """
    Calculate age and assign age groups based on birth date.
    
    Args:
        df: DataFrame containing birth date information
        birth_date_col: Name of the birth date column
        
    Returns:
        DataFrame with added 'age' and 'age_group' columns
    """
    df = df.copy()
    current_date = datetime.now()
    
    # Calculate age
    df['age'] = df[birth_date_col].apply(
        lambda x: current_date.year - x.year - 
        ((current_date.month, current_date.day) < (x.month, x.day)) if pd.notna(x) else np.nan
    )
    
    # Define age groups
    def assign_age_group(age):
        if pd.isna(age):
            return "Unknown"
        elif age <= 18:
            return "0-18"
        elif age <= 30:
            return "19-30"
        elif age <= 50:
            return "31-50"
        elif age <= 65:
            return "51-65"
        else:
            return "65+"
    
    df['age_group'] = df['age'].apply(assign_age_group)
    return df


def assign_data_sources(df: pd.DataFrame, source_column: str) -> pd.DataFrame:
    """
    Assign readable source names based on source codes.
    
    Args:
        df: DataFrame containing source information
        source_column: Name of the source code column
        
    Returns:
        DataFrame with added 'source_name' and 'channel_type' columns
    """
    df = df.copy()
    df["source_name"] = ""
    df["channel_type"] = ""
    
    # Source mapping logic (simplified for demonstration)
    source_mapping = {
        'amazon': 'Amazon',
        'google': 'Google',
        'newsletter': 'Newsletter',
        'social': 'Social Media',
        'offline': 'Offline'
    }
    
    # Apply source mapping (simplified logic)
    for pattern, source_name in source_mapping.items():
        mask = df[source_column].str.contains(pattern, case=False, na=False)
        df.loc[mask, "source_name"] = source_name
    
    # Assign channel type
    online_sources = ['Amazon', 'Google', 'Newsletter', 'Social Media']
    df.loc[df["source_name"].isin(online_sources), "channel_type"] = "Online"
    df.loc[df["source_name"].isin(['Offline']), "channel_type"] = "Offline"
    
    return df


def process_salutation(df: pd.DataFrame, salutation_col: str = "salutation") -> pd.DataFrame:
    """
    Process and standardize salutation values.
    
    Args:
        df: DataFrame containing salutation information
        salutation_col: Name of the salutation column
        
    Returns:
        DataFrame with processed salutation column
    """
    df = df.copy()
    
    # Salutation mapping
    salutation_mapping = {
        '1': 'Mr.',
        '2': 'Ms.', 
        '3': 'Mr./Ms.',
        '4': 'Company',
        '5': 'Company Address',
        '6': 'Miss',
        '7': 'Family',
        'X': 'Diverse'
    }
    
    def clean_salutation(value):
        if pd.isna(value):
            return ""
        value = str(value)
        # Remove leading zeros and .0 suffix
        if value.startswith('0'):
            value = value.lstrip('0')
        if value.endswith('.0'):
            value = value[:-2]
        return value
    
    df[salutation_col] = df[salutation_col].apply(clean_salutation)
    df[salutation_col] = df[salutation_col].map(salutation_mapping).fillna("")
    
    return df


def calculate_net_sales(df: pd.DataFrame, 
                       gross_amount_col: str = "gross_amount",
                       tax1_col: str = "tax1", 
                       tax2_col: str = "tax2",
                       tax3_col: str = "tax3") -> pd.DataFrame:
    """
    Calculate net sales amount by subtracting taxes from gross amount.
    
    Args:
        df: DataFrame containing sales and tax information
        gross_amount_col: Name of the gross amount column
        tax1_col: Name of the first tax column
        tax2_col: Name of the second tax column  
        tax3_col: Name of the third tax column
        
    Returns:
        DataFrame with added 'net_amount' column
    """
    df = df.copy()
    
    df["net_amount"] = (
        df[gross_amount_col] 
        - df[tax1_col].fillna(0) 
        - df[tax2_col].fillna(0) 
        - df[tax3_col].fillna(0)
    )
    
    return df


def get_half_year_reference_dates(today: date = None) -> tuple[date, date, date]:
    """
    Calculate reference dates for half-year analysis.
    
    Args:
        today: Reference date (defaults to current date)
        
    Returns:
        Tuple of (five_years_ago_start, two_years_ago_start, today)
    """
    if today is None:
        today = date.today()
    
    # Determine if we're in H1 or H2
    if today.month <= 6:
        month_start = 1
    else:
        month_start = 7
    
    # Calculate reference dates
    five_years_ago_start = date(today.year - 5, month_start, 1)
    two_years_ago_start = date(today.year - 2, month_start, 1)
    
    return five_years_ago_start, two_years_ago_start, today


def validate_data_quality(df: pd.DataFrame, required_columns: list) -> Dict[str, Any]:
    """
    Perform basic data quality checks on a DataFrame.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        Dictionary containing validation results
    """
    results = {
        'total_rows': len(df),
        'missing_columns': [],
        'null_counts': {},
        'duplicate_rows': 0
    }
    
    # Check for missing columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    results['missing_columns'] = missing_cols
    
    # Check for null values in required columns
    for col in required_columns:
        if col in df.columns:
            results['null_counts'][col] = df[col].isnull().sum()
    
    # Check for duplicate rows
    results['duplicate_rows'] = df.duplicated().sum()
    
    return results


def safe_string_operations(series: pd.Series) -> pd.Series:
    """
    Safely perform string operations on a pandas Series.
    
    Args:
        series: Pandas Series to process
        
    Returns:
        Series with safe string operations applied
    """
    return series.astype("string").fillna("")


def create_customer_segments(df: pd.DataFrame, 
                           recency_col: str = "recency",
                           frequency_col: str = "frequency", 
                           monetary_col: str = "monetary") -> pd.DataFrame:
    """
    Create customer segments based on RFM analysis.
    
    Args:
        df: DataFrame containing RFM scores
        recency_col: Name of the recency score column
        frequency_col: Name of the frequency score column
        monetary_col: Name of the monetary score column
        
    Returns:
        DataFrame with added 'customer_segment' column
    """
    df = df.copy()
    
    def assign_segment(row):
        r, f, m = row[recency_col], row[frequency_col], row[monetary_col]
        
        if pd.isna(r) or pd.isna(f) or pd.isna(m):
            return "Unknown"
        
        # Simple segmentation logic
        if r >= 4 and f >= 4 and m >= 4:
            return "Champions"
        elif r >= 3 and f >= 3 and m >= 3:
            return "Loyal Customers"
        elif r >= 2 and f >= 2:
            return "Potential Loyalists"
        elif r >= 3 and f <= 2:
            return "New Customers"
        elif r <= 2 and f >= 3:
            return "At Risk"
        else:
            return "Lost Customers"
    
    df['customer_segment'] = df.apply(assign_segment, axis=1)
    return df
