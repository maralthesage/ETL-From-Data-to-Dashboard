"""
Customer Data ETL Pipeline

This module contains the main ETL pipeline for processing customer data.
It extracts data from multiple sources, transforms it, and loads it into
a format suitable for analytics and reporting.
"""

import os
import time
import pandas as pd
from pathlib import Path
from prefect import flow, task, get_run_logger
from typing import Dict, Any

from config.settings import (
    COUNTRY_MAPPING, SUPPORTED_COUNTRIES, CSV_READ_KWARGS, 
    OUTPUT_PATH, OUTPUT_FILES
)
from src.utils.data_processing import (
    pad_customer_id, calculate_age_groups, assign_data_sources,
    process_salutation, calculate_net_sales, validate_data_quality
)


@task
def load_customer_data(country: str, data_path: str) -> Dict[str, pd.DataFrame]:
    """
    Load customer data from various sources for a specific country.
    
    Args:
        country: Country code (e.g., 'F01', 'F02')
        data_path: Base path to data files
        
    Returns:
        Dictionary containing loaded DataFrames
    """
    logger = get_run_logger()
    logger.info(f"Loading data for country: {country}")
    
    country_path = Path(data_path) / country
    
    try:
        # Load customer addresses
        addresses = pd.read_csv(
            country_path / 'customer_addresses.csv',
            **CSV_READ_KWARGS,
            parse_dates=["registration_date", "birth_date"],
            usecols=[
                "customer_id", "salutation", "title", "first_name", "last_name",
                "source", "postal_code", "city", "marketing_consent", 
                "phone", "registration_date", "birth_date"
            ]
        )
        
        # Load customer transactions
        transactions = pd.read_csv(
            country_path / 'customer_transactions.csv',
            **CSV_READ_KWARGS,
            parse_dates=["transaction_date"],
            usecols=[
                "invoice_number", "order_number", "customer_reference", 
                "transaction_date", "gross_amount", "tax1", "tax2", "tax3"
            ]
        )
        
        # Load customer emails
        emails = pd.read_csv(
            country_path / 'customer_emails.csv',
            **CSV_READ_KWARGS,
            usecols=["customer_id", "email_address", "is_primary"]
        )
        
        # Load advertising data
        advertising = pd.read_csv(
            country_path / 'advertising_data.csv',
            **CSV_READ_KWARGS,
            parse_dates=["advertising_date"],
            usecols=["customer_id", "advertising_date", "media_code"]
        )
        
        logger.info(f"Successfully loaded data for {country}")
        logger.info(f"Addresses: {len(addresses):,} rows")
        logger.info(f"Transactions: {len(transactions):,} rows")
        logger.info(f"Emails: {len(emails):,} rows")
        logger.info(f"Advertising: {len(advertising):,} rows")
        
        return {
            'addresses': addresses,
            'transactions': transactions,
            'emails': emails,
            'advertising': advertising
        }
        
    except Exception as e:
        logger.error(f"Error loading data for {country}: {str(e)}")
        raise


@task
def transform_customer_data(data: Dict[str, pd.DataFrame], country: str) -> pd.DataFrame:
    """
    Transform and merge customer data from multiple sources.
    
    Args:
        data: Dictionary containing source DataFrames
        country: Country code
        
    Returns:
        Merged and transformed DataFrame
    """
    logger = get_run_logger()
    logger.info(f"Transforming data for {country}")
    
    addresses = data['addresses']
    transactions = data['transactions']
    emails = data['emails']
    advertising = data['advertising']
    
    # Process customer IDs
    addresses = pad_customer_id(addresses, "customer_id")
    transactions = pad_customer_id(transactions, "customer_reference")
    emails = pad_customer_id(emails, "customer_id")
    advertising = pad_customer_id(advertising, "customer_id")
    
    # Calculate net sales
    transactions = calculate_net_sales(
        transactions, 
        gross_amount_col="gross_amount",
        tax1_col="tax1",
        tax2_col="tax2", 
        tax3_col="tax3"
    )
    
    # Extract customer ID from transaction reference
    transactions['customer_id'] = transactions['customer_reference'].str[2:12]
    
    # Process addresses
    addresses = calculate_age_groups(addresses, "birth_date")
    addresses = assign_data_sources(addresses, "source")
    addresses = process_salutation(addresses, "salutation")
    
    # Get latest advertising per customer
    advertising_latest = (
        advertising.groupby('customer_id')
        .agg({
            'advertising_date': 'max',
            'media_code': 'last'
        })
        .reset_index()
        .rename(columns={
            'advertising_date': 'last_advertising_date',
            'media_code': 'last_media_code'
        })
    )
    
    # Merge all data
    merged_data = addresses.merge(emails, on="customer_id", how="left")
    merged_data = merged_data.merge(transactions, on="customer_id", how="left")
    merged_data = merged_data.merge(advertising_latest, on="customer_id", how="left")
    
    # Add country information
    merged_data['country'] = COUNTRY_MAPPING.get(country, country)
    
    logger.info(f"Transformed data: {len(merged_data):,} rows")
    return merged_data


@task
def aggregate_customer_data(merged_data: pd.DataFrame, country: str) -> pd.DataFrame:
    """
    Aggregate customer data for analytics and reporting.
    
    Args:
        merged_data: Merged customer data
        country: Country code
        
    Returns:
        Aggregated customer data
    """
    logger = get_run_logger()
    logger.info(f"Aggregating data for {country}")
    
    # Group by customer and order
    customer_orders = (
        merged_data.groupby(["customer_id", "order_number"])
        .agg({
            'salutation': 'first',
            'first_name': 'first', 
            'last_name': 'first',
            'age_group': 'first',
            'city': 'first',
            'registration_date': 'first',
            'source_name': 'first',
            'channel_type': 'first',
            'transaction_date': 'min',
            'net_amount': 'sum',
            'last_advertising_date': 'max',
            'last_media_code': 'last'
        })
        .reset_index()
    )
    
    # Handle customers without orders
    customers_without_orders = merged_data[merged_data['order_number'].isna()]
    if not customers_without_orders.empty:
        customers_without_orders = (
            customers_without_orders.groupby("customer_id")
            .agg({
                'salutation': 'first',
                'first_name': 'first',
                'last_name': 'first', 
                'age_group': 'first',
                'city': 'first',
                'registration_date': 'first',
                'source_name': 'first',
                'channel_type': 'first',
                'net_amount': 'sum'
            })
            .reset_index()
        )
        customers_without_orders['order_number'] = None
        
        # Combine both groups
        final_data = pd.concat([customer_orders, customers_without_orders])
    else:
        final_data = customer_orders
    
    # Clean up and rename columns
    final_data = final_data.rename(columns={
        'order_number': 'order_id',
        'transaction_date': 'order_date',
        'net_amount': 'total_net_sales',
        'last_advertising_date': 'last_advertising_date',
        'last_media_code': 'last_advertising_media'
    })
    
    # Remove duplicates
    final_data = final_data.drop_duplicates(subset=["customer_id", "order_id"])
    final_data['country'] = COUNTRY_MAPPING.get(country, country)
    
    logger.info(f"Aggregated data: {len(final_data):,} rows")
    return final_data


@task
def validate_output_data(data: pd.DataFrame, country: str) -> Dict[str, Any]:
    """
    Validate the output data quality.
    
    Args:
        data: Final processed data
        country: Country code
        
    Returns:
        Validation results
    """
    logger = get_run_logger()
    
    required_columns = [
        'customer_id', 'salutation', 'first_name', 'last_name',
        'age_group', 'city', 'total_net_sales', 'country'
    ]
    
    validation_results = validate_data_quality(data, required_columns)
    
    logger.info(f"Data validation for {country}:")
    logger.info(f"Total rows: {validation_results['total_rows']:,}")
    logger.info(f"Missing columns: {validation_results['missing_columns']}")
    logger.info(f"Duplicate rows: {validation_results['duplicate_rows']}")
    
    return validation_results


@task
def save_processed_data(data: pd.DataFrame, country: str, output_path: str) -> str:
    """
    Save processed data to output file.
    
    Args:
        data: Processed customer data
        country: Country code
        output_path: Output directory path
        
    Returns:
        Path to saved file
    """
    logger = get_run_logger()
    
    # Create output directory
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filename
    filename = OUTPUT_FILES['customer_analysis'].format(country=country)
    filepath = output_dir / filename
    
    # Save data
    data.to_csv(filepath, index=False, sep=";", encoding="cp850")
    
    logger.info(f"Saved {len(data):,} rows to {filepath}")
    return str(filepath)


@flow(name="customer-etl-pipeline")
def run_customer_etl_pipeline(
    country: str = "F01",
    data_path: str = None,
    output_path: str = None
) -> Dict[str, Any]:
    """
    Main ETL pipeline for customer data processing.
    
    Args:
        country: Country code to process
        data_path: Path to source data files
        output_path: Path for output files
        
    Returns:
        Dictionary containing processing results
    """
    logger = get_run_logger()
    start_time = time.time()
    
    logger.info(f"Starting customer ETL pipeline for {country}")
    
    # Use default paths if not provided
    if data_path is None:
        data_path = os.getenv("DATA_PATH", "/path/to/your/data")
    if output_path is None:
        output_path = OUTPUT_PATH
    
    try:
        # Load data
        data = load_customer_data(country, data_path)
        
        # Transform data
        transformed_data = transform_customer_data(data, country)
        
        # Aggregate data
        aggregated_data = aggregate_customer_data(transformed_data, country)
        
        # Validate data
        validation_results = validate_output_data(aggregated_data, country)
        
        # Save data
        output_file = save_processed_data(aggregated_data, country, output_path)
        
        # Calculate summary statistics
        total_customers = aggregated_data['customer_id'].nunique()
        total_sales = aggregated_data['total_net_sales'].fillna(0).sum()
        
        processing_time = time.time() - start_time
        
        results = {
            'country': country,
            'total_customers': total_customers,
            'total_sales': total_sales,
            'processing_time': processing_time,
            'output_file': output_file,
            'validation_results': validation_results
        }
        
        logger.info(f"ETL pipeline completed for {country}")
        logger.info(f"Processed {total_customers:,} customers")
        logger.info(f"Total sales: {total_sales:,.2f}")
        logger.info(f"Processing time: {processing_time:.2f} seconds")
        
        return results
        
    except Exception as e:
        logger.error(f"ETL pipeline failed for {country}: {str(e)}")
        raise


if __name__ == "__main__":
    # Run pipeline for all supported countries
    for country in SUPPORTED_COUNTRIES:
        run_customer_etl_pipeline(country=country)
