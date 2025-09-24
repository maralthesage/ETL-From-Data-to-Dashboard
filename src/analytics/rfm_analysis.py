"""
RFM Analysis Pipeline

This module implements RFM (Recency, Frequency, Monetary) analysis for customer segmentation.
It processes customer transaction data to create meaningful customer segments.
"""

import pandas as pd
import numpy as np
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from prefect import flow, task, get_run_logger
from typing import Dict, List, Tuple

from config.settings import RFM_CONFIG, CUSTOMER_SEGMENTS
from src.utils.data_processing import (
    pad_customer_id, get_half_year_reference_dates, 
    create_customer_segments, validate_data_quality
)


@task
def load_rfm_data(country: str, data_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load data required for RFM analysis.
    
    Args:
        country: Country code
        data_path: Base path to data files
        
    Returns:
        Tuple of (addresses, transactions, email_types, customer_groups)
    """
    logger = get_run_logger()
    logger.info(f"Loading RFM data for {country}")
    
    country_path = f"{data_path}/{country}"
    
    # Load customer addresses
    addresses = pd.read_csv(
        f"{country_path}/customer_addresses.csv",
        sep=";",
        encoding="cp850",
        usecols=["customer_id", "registration_date", "source"],
        parse_dates=["registration_date"]
    )
    
    # Load customer transactions
    transactions = pd.read_csv(
        f"{country_path}/customer_transactions.csv", 
        sep=";",
        encoding="cp850",
        usecols=[
            "customer_reference", "order_number", "gross_amount",
            "tax1", "tax2", "tax3", "transaction_date"
        ],
        parse_dates=["transaction_date"]
    )
    
    # Load email preferences
    email_types = pd.read_csv(
        f"{country_path}/email_preferences.csv",
        sep=";", 
        encoding="cp850",
        usecols=["customer_id", "email_type"]
    )
    
    # Load existing customer groups
    customer_groups = pd.read_csv(
        f"{data_path}/customer_groups/customer_groups_{country}.csv",
        sep=";",
        encoding="cp850", 
        usecols=["customer_id", "customer_group"]
    )
    
    logger.info(f"Loaded data: addresses={len(addresses):,}, transactions={len(transactions):,}")
    
    return addresses, transactions, email_types, customer_groups


@task
def clean_rfm_data(
    addresses: pd.DataFrame,
    transactions: pd.DataFrame, 
    email_types: pd.DataFrame,
    customer_groups: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Clean and prepare data for RFM analysis.
    
    Args:
        addresses: Customer address data
        transactions: Transaction data
        email_types: Email preference data
        customer_groups: Customer group data
        
    Returns:
        Cleaned data tuples
    """
    logger = get_run_logger()
    logger.info("Cleaning RFM data")
    
    # Clean customer IDs
    addresses = pad_customer_id(addresses, "customer_id")
    email_types = pad_customer_id(email_types, "customer_id")
    customer_groups = pad_customer_id(customer_groups, "customer_id")
    
    # Extract customer ID from transaction reference
    transactions = transactions.copy()
    transactions['customer_id'] = transactions['customer_reference'].str[2:12]
    
    # Calculate net sales
    transactions['net_sales'] = (
        transactions['gross_amount'] 
        - transactions['tax1'].fillna(0)
        - transactions['tax2'].fillna(0) 
        - transactions['tax3'].fillna(0)
    )
    
    # Clean dates
    addresses['registration_date'] = pd.to_datetime(addresses['registration_date'], errors='coerce')
    transactions['transaction_date'] = pd.to_datetime(transactions['transaction_date'], errors='coerce')
    
    logger.info("Data cleaning completed")
    return addresses, transactions, email_types, customer_groups


@task
def merge_rfm_data(
    addresses: pd.DataFrame,
    transactions: pd.DataFrame,
    email_types: pd.DataFrame,
    half_year_info: Dict
) -> pd.DataFrame:
    """
    Merge data sources for RFM analysis.
    
    Args:
        addresses: Customer address data
        transactions: Transaction data
        email_types: Email preference data
        half_year_info: Half-year information
        
    Returns:
        Merged DataFrame
    """
    logger = get_run_logger()
    logger.info("Merging RFM data")
    
    # Filter transactions by half-year end date
    prev_end = pd.Timestamp(half_year_info["prev_end"])
    transactions = transactions[transactions['transaction_date'] <= prev_end]
    
    # Merge addresses with transactions
    merged = addresses.merge(
        transactions[["customer_id", "order_number", "net_sales", "transaction_date"]],
        on="customer_id", 
        how="left"
    )
    
    # Merge with email types
    merged = merged.merge(email_types, on="customer_id", how="left")
    
    # Select relevant columns
    merged = merged[[
        "customer_id", "registration_date", "email_type", 
        "order_number", "net_sales", "transaction_date"
    ]]
    
    logger.info(f"Merged data: {len(merged):,} rows")
    return merged


@task
def calculate_customer_metrics(merged_data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate customer-level metrics for RFM analysis.
    
    Args:
        merged_data: Merged customer and transaction data
        
    Returns:
        DataFrame with customer metrics
    """
    logger = get_run_logger()
    logger.info("Calculating customer metrics")
    
    # Group by customer
    customer_metrics = (
        merged_data.groupby("customer_id", as_index=False)
        .agg({
            'email_type': 'first',
            'registration_date': 'first',
            'order_number': 'nunique',  # Frequency
            'net_sales': 'sum',        # Monetary
            'transaction_date': 'max'  # Recency
        })
        .rename(columns={
            'order_number': 'frequency',
            'net_sales': 'monetary',
            'transaction_date': 'recency'
        })
    )
    
    # Handle customers with no transactions
    customer_metrics['frequency'] = customer_metrics['frequency'].fillna(0)
    customer_metrics['monetary'] = customer_metrics['monetary'].fillna(0)
    
    logger.info(f"Calculated metrics for {len(customer_metrics):,} customers")
    return customer_metrics


@task
def calculate_time_period_metrics(
    merged_data: pd.DataFrame,
    five_years_ago: pd.Timestamp,
    two_years_ago: pd.Timestamp,
    today: pd.Timestamp
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calculate metrics for different time periods.
    
    Args:
        merged_data: Merged customer data
        five_years_ago: Five years ago timestamp
        two_years_ago: Two years ago timestamp  
        today: Current timestamp
        
    Returns:
        Tuple of (3-5 years metrics, last 2 years metrics)
    """
    logger = get_run_logger()
    logger.info("Calculating time period metrics")
    
    # Filter for 3-5 years ago
    mask_3to5 = (
        (merged_data['transaction_date'] >= five_years_ago) & 
        (merged_data['transaction_date'] < two_years_ago)
    )
    data_3to5 = merged_data[mask_3to5]
    
    # Filter for last 2 years
    mask_last2 = (
        (merged_data['transaction_date'] >= two_years_ago) & 
        (merged_data['transaction_date'] < today)
    )
    data_last2 = merged_data[mask_last2]
    
    # Calculate metrics for 3-5 years ago
    metrics_3to5 = (
        data_3to5.groupby("customer_id", as_index=False)
        .agg({
            'order_number': 'nunique',
            'net_sales': 'sum'
        })
        .rename(columns={
            'order_number': 'freq_3to5_years',
            'net_sales': 'monetary_3to5_years'
        })
    )
    
    # Calculate metrics for last 2 years
    metrics_last2 = (
        data_last2.groupby("customer_id", as_index=False)
        .agg({
            'order_number': 'nunique', 
            'net_sales': 'sum'
        })
        .rename(columns={
            'order_number': 'freq_last_2_years',
            'net_sales': 'monetary_last_2_years'
        })
    )
    
    logger.info(f"Time period metrics calculated")
    return metrics_3to5, metrics_last2


@task
def calculate_rfm_scores(
    customer_metrics: pd.DataFrame,
    metrics_3to5: pd.DataFrame,
    metrics_last2: pd.DataFrame,
    reference_date: pd.Timestamp
) -> pd.DataFrame:
    """
    Calculate RFM scores for customer segmentation.
    
    Args:
        customer_metrics: Base customer metrics
        metrics_3to5: 3-5 years ago metrics
        metrics_last2: Last 2 years metrics
        reference_date: Reference date for recency calculation
        
    Returns:
        DataFrame with RFM scores
    """
    logger = get_run_logger()
    logger.info("Calculating RFM scores")
    
    # Merge time period metrics
    rfm_data = customer_metrics.merge(metrics_3to5, on="customer_id", how="left")
    rfm_data = rfm_data.merge(metrics_last2, on="customer_id", how="left")
    
    # Fill missing values
    for col in ['freq_3to5_years', 'freq_last_2_years']:
        rfm_data[col] = rfm_data[col].fillna(0)
    for col in ['monetary_3to5_years', 'monetary_last_2_years']:
        rfm_data[col] = rfm_data[col].fillna(0)
    
    # Calculate weighted 5-year metrics
    rfm_data['freq_5_years'] = (
        (rfm_data['freq_3to5_years'] * 0.5) + rfm_data['freq_last_2_years']
    ).round()
    
    rfm_data['monetary_5_years'] = (
        (rfm_data['monetary_3to5_years'] * 0.5) + rfm_data['monetary_last_2_years']
    ).round()
    
    # Calculate recency score (days since last purchase)
    rfm_data['recency_days'] = (reference_date - rfm_data['recency']).dt.days
    rfm_data['recency_days'] = rfm_data['recency_days'].fillna(9999)  # No purchases
    
    # Calculate RFM scores using bins
    monetary_bins = RFM_CONFIG['MONETARY_BINS']
    monetary_labels = RFM_CONFIG['MONETARY_LABELS']
    freq_bins = RFM_CONFIG['FREQ_BINS'] 
    freq_labels = RFM_CONFIG['FREQ_LABELS']
    
    # Monetary score
    rfm_data['m_score'] = pd.cut(
        rfm_data['monetary_5_years'],
        bins=monetary_bins,
        labels=monetary_labels,
        include_lowest=True,
        right=False
    ).astype(int)
    
    # Frequency score
    rfm_data['f_score'] = pd.cut(
        rfm_data['freq_5_years'],
        bins=freq_bins,
        labels=freq_labels,
        include_lowest=True
    ).astype(int)
    
    # Recency score (inverse - lower days = higher score)
    recency_bins = [0, 30, 90, 180, 365, float('inf')]
    recency_labels = [5, 4, 3, 2, 1]
    rfm_data['r_score'] = pd.cut(
        rfm_data['recency_days'],
        bins=recency_bins,
        labels=recency_labels,
        include_lowest=True
    ).astype(int)
    
    # Combined MF score
    rfm_data['mf_score'] = ((rfm_data['m_score'] + rfm_data['f_score']) / 2).round()
    
    logger.info("RFM scores calculated")
    return rfm_data


@task
def assign_customer_segments(rfm_data: pd.DataFrame) -> pd.DataFrame:
    """
    Assign customer segments based on RFM scores.
    
    Args:
        rfm_data: DataFrame with RFM scores
        
    Returns:
        DataFrame with customer segments
    """
    logger = get_run_logger()
    logger.info("Assigning customer segments")
    
    def assign_segment(row):
        r, f, mf = row['r_score'], row['f_score'], row['mf_score']
        
        if pd.isna(r) or pd.isna(f) or pd.isna(mf):
            return "Unknown"
        
        # RFM segmentation logic
        if mf >= 4 and r >= 4:
            return "01-Champions"
        elif mf >= 4 and r in [2, 3]:
            return "02-Treue Kunden"
        elif mf == 5 and r <= 1:
            return "03-Nicht zu verlieren"
        elif mf in [2, 3] and r >= 3:
            return "04-Potenziell loyale Kunden"
        elif mf == 3 and r in [2, 3]:
            return "05-Brauchen Aufmerksamkeit"
        elif mf >= 3 and r <= 1:
            return "06-Gefährdete Kunden"
        elif mf == 1 and r >= 4:
            return "08-Reaktivierte Kunden"
        elif mf == 1 and r in [2, 3]:
            return "09-Vielversprechende Kunden"
        elif mf <= 2 and r in [2, 3]:
            return "10-Abwandernde Kunden"
        elif mf == 2 and r <= 1:
            return "11-Schlafende Kunden"
        elif mf == 1 and r <= 1:
            return "12-Verlorene Kunden"
        else:
            return "Nicht klassifiziert"
    
    rfm_data['customer_segment'] = rfm_data.apply(assign_segment, axis=1)
    
    # Special cases
    rfm_data.loc[rfm_data['monetary'] == 0, 'customer_segment'] = "13-Interessenten"
    
    logger.info("Customer segments assigned")
    return rfm_data


@task
def export_rfm_results(rfm_data: pd.DataFrame, customer_groups: pd.DataFrame, country: str) -> str:
    """
    Export RFM analysis results.
    
    Args:
        rfm_data: RFM analysis results
        customer_groups: Customer group data
        country: Country code
        
    Returns:
        Path to exported file
    """
    logger = get_run_logger()
    logger.info(f"Exporting RFM results for {country}")
    
    # Merge with customer groups
    final_data = rfm_data.merge(customer_groups, on="customer_id", how="left")
    final_data = final_data.drop_duplicates(subset="customer_id")
    
    # Create output directory
    output_dir = f"data/output/rfm_analysis"
    os.makedirs(output_dir, exist_ok=True)
    
    # Export results
    output_file = f"{output_dir}/rfm_segments_{country}.csv"
    final_data.to_csv(output_file, index=False, sep=";", encoding="cp850")
    
    logger.info(f"Exported {len(final_data):,} customer segments to {output_file}")
    return output_file


@flow(name="rfm-analysis-pipeline")
def run_rfm_analysis(
    country: str = "F01",
    data_path: str = None
) -> Dict[str, Any]:
    """
    Main RFM analysis pipeline.
    
    Args:
        country: Country code to analyze
        data_path: Path to source data
        
    Returns:
        Analysis results
    """
    logger = get_run_logger()
    logger.info(f"Starting RFM analysis for {country}")
    
    if data_path is None:
        data_path = os.getenv("DATA_PATH", "/path/to/your/data")
    
    try:
        # Get reference dates
        five_years_ago, two_years_ago, today = get_half_year_reference_dates()
        
        # Load data
        addresses, transactions, email_types, customer_groups = load_rfm_data(country, data_path)
        
        # Clean data
        addresses, transactions, email_types, customer_groups = clean_rfm_data(
            addresses, transactions, email_types, customer_groups
        )
        
        # Get half-year info
        half_year_info = {
            "prev_start": five_years_ago,
            "prev_end": two_years_ago
        }
        
        # Merge data
        merged_data = merge_rfm_data(addresses, transactions, email_types, half_year_info)
        
        # Calculate customer metrics
        customer_metrics = calculate_customer_metrics(merged_data)
        
        # Calculate time period metrics
        metrics_3to5, metrics_last2 = calculate_time_period_metrics(
            merged_data, five_years_ago, two_years_ago, today
        )
        
        # Calculate RFM scores
        rfm_data = calculate_rfm_scores(
            customer_metrics, metrics_3to5, metrics_last2, today
        )
        
        # Assign segments
        rfm_data = assign_customer_segments(rfm_data)
        
        # Export results
        output_file = export_rfm_results(rfm_data, customer_groups, country)
        
        # Summary statistics
        segment_counts = rfm_data['customer_segment'].value_counts()
        
        results = {
            'country': country,
            'total_customers': len(rfm_data),
            'segment_distribution': segment_counts.to_dict(),
            'output_file': output_file
        }
        
        logger.info(f"RFM analysis completed for {country}")
        logger.info(f"Total customers: {len(rfm_data):,}")
        logger.info(f"Segment distribution: {segment_counts.to_dict()}")
        
        return results
        
    except Exception as e:
        logger.error(f"RFM analysis failed for {country}: {str(e)}")
        raise


if __name__ == "__main__":
    # Run RFM analysis for all countries
    from config.settings import SUPPORTED_COUNTRIES
    
    for country in SUPPORTED_COUNTRIES:
        run_rfm_analysis(country=country)
