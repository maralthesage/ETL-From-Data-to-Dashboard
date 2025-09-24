"""
Main orchestration script for the ETL pipeline.

This script coordinates the execution of all ETL processes including
customer data processing, RFM analysis, and customer segmentation.

Copyright (c) 2025 ETL Pipeline Project
Licensed under the MIT License - see LICENSE file for details.
"""

import os
import time
from datetime import datetime
from prefect import flow, get_run_logger
from typing import Dict, Any, List

from config.settings import SUPPORTED_COUNTRIES
from src.etl.customer_pipeline import run_customer_etl_pipeline
from src.analytics.rfm_analysis import run_rfm_analysis


@flow(name="complete-etl-pipeline")
def run_complete_etl_pipeline(
    countries: List[str] = None,
    data_path: str = None,
    output_path: str = None
) -> Dict[str, Any]:
    """
    Run the complete ETL pipeline for all specified countries.
    
    Args:
        countries: List of country codes to process
        data_path: Path to source data files
        output_path: Path for output files
        
    Returns:
        Dictionary containing processing results for all countries
    """
    logger = get_run_logger()
    start_time = time.time()
    
    if countries is None:
        countries = SUPPORTED_COUNTRIES
    
    logger.info(f"Starting complete ETL pipeline for countries: {countries}")
    
    results = {
        'start_time': datetime.now().isoformat(),
        'countries_processed': [],
        'total_processing_time': 0,
        'successful_countries': [],
        'failed_countries': []
    }
    
    for country in countries:
        country_start_time = time.time()
        
        try:
            logger.info(f"Processing country: {country}")
            
            # Run customer ETL pipeline
            customer_results = run_customer_etl_pipeline(
                country=country,
                data_path=data_path,
                output_path=output_path
            )
            
            # Run RFM analysis
            rfm_results = run_rfm_analysis(
                country=country,
                data_path=data_path
            )
            
            country_processing_time = time.time() - country_start_time
            
            results['countries_processed'].append({
                'country': country,
                'customer_etl': customer_results,
                'rfm_analysis': rfm_results,
                'processing_time': country_processing_time
            })
            
            results['successful_countries'].append(country)
            
            logger.info(f"Successfully processed {country} in {country_processing_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Failed to process {country}: {str(e)}")
            results['failed_countries'].append({
                'country': country,
                'error': str(e)
            })
    
    total_processing_time = time.time() - start_time
    results['total_processing_time'] = total_processing_time
    results['end_time'] = datetime.now().isoformat()
    
    # Summary
    logger.info("ETL Pipeline Summary:")
    logger.info(f"Total processing time: {total_processing_time:.2f} seconds")
    logger.info(f"Successful countries: {len(results['successful_countries'])}")
    logger.info(f"Failed countries: {len(results['failed_countries'])}")
    logger.info(f"Successful: {results['successful_countries']}")
    if results['failed_countries']:
        logger.warning(f"Failed: {[f['country'] for f in results['failed_countries']]}")
    
    return results


@flow(name="customer-data-only")
def run_customer_data_pipeline(
    countries: List[str] = None,
    data_path: str = None,
    output_path: str = None
) -> Dict[str, Any]:
    """
    Run only the customer data ETL pipeline.
    
    Args:
        countries: List of country codes to process
        data_path: Path to source data files
        output_path: Path for output files
        
    Returns:
        Dictionary containing processing results
    """
    logger = get_run_logger()
    start_time = time.time()
    
    if countries is None:
        countries = SUPPORTED_COUNTRIES
    
    logger.info(f"Running customer data pipeline for: {countries}")
    
    results = {
        'start_time': datetime.now().isoformat(),
        'countries_processed': [],
        'total_processing_time': 0
    }
    
    for country in countries:
        try:
            country_results = run_customer_etl_pipeline(
                country=country,
                data_path=data_path,
                output_path=output_path
            )
            
            results['countries_processed'].append({
                'country': country,
                'results': country_results
            })
            
            logger.info(f"Successfully processed customer data for {country}")
            
        except Exception as e:
            logger.error(f"Failed to process customer data for {country}: {str(e)}")
    
    results['total_processing_time'] = time.time() - start_time
    results['end_time'] = datetime.now().isoformat()
    
    return results


@flow(name="rfm-analysis-only")
def run_rfm_analysis_pipeline(
    countries: List[str] = None,
    data_path: str = None
) -> Dict[str, Any]:
    """
    Run only the RFM analysis pipeline.
    
    Args:
        countries: List of country codes to analyze
        data_path: Path to source data files
        
    Returns:
        Dictionary containing analysis results
    """
    logger = get_run_logger()
    start_time = time.time()
    
    if countries is None:
        countries = SUPPORTED_COUNTRIES
    
    logger.info(f"Running RFM analysis for: {countries}")
    
    results = {
        'start_time': datetime.now().isoformat(),
        'countries_processed': [],
        'total_processing_time': 0
    }
    
    for country in countries:
        try:
            country_results = run_rfm_analysis(
                country=country,
                data_path=data_path
            )
            
            results['countries_processed'].append({
                'country': country,
                'results': country_results
            })
            
            logger.info(f"Successfully completed RFM analysis for {country}")
            
        except Exception as e:
            logger.error(f"Failed RFM analysis for {country}: {str(e)}")
    
    results['total_processing_time'] = time.time() - start_time
    results['end_time'] = datetime.now().isoformat()
    
    return results


def main():
    """
    Main entry point for the ETL pipeline.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="ETL Pipeline for Customer Data Processing")
    parser.add_argument(
        "--mode", 
        choices=["complete", "customer", "rfm"], 
        default="complete",
        help="Pipeline mode to run"
    )
    parser.add_argument(
        "--countries",
        nargs="+",
        default=SUPPORTED_COUNTRIES,
        help="Countries to process"
    )
    parser.add_argument(
        "--data-path",
        default=os.getenv("DATA_PATH", "/path/to/your/data"),
        help="Path to source data files"
    )
    parser.add_argument(
        "--output-path", 
        default=os.getenv("OUTPUT_PATH", "/path/to/your/output"),
        help="Path for output files"
    )
    
    args = parser.parse_args()
    
    if args.mode == "complete":
        run_complete_etl_pipeline(
            countries=args.countries,
            data_path=args.data_path,
            output_path=args.output_path
        )
    elif args.mode == "customer":
        run_customer_data_pipeline(
            countries=args.countries,
            data_path=args.data_path,
            output_path=args.output_path
        )
    elif args.mode == "rfm":
        run_rfm_analysis_pipeline(
            countries=args.countries,
            data_path=args.data_path
        )


if __name__ == "__main__":
    main()
