"""
Test cases for data processing utilities.

This module contains unit tests for the data processing utilities
to ensure data quality and processing accuracy.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date
from src.utils.data_processing import (
    pad_customer_id,
    calculate_age_groups,
    assign_data_sources,
    process_salutation,
    calculate_net_sales,
    validate_data_quality,
    create_customer_segments
)


class TestDataProcessing:
    """Test cases for data processing utilities."""
    
    def test_pad_customer_id(self):
        """Test customer ID padding functionality."""
        # Test data
        df = pd.DataFrame({
            'customer_id': ['123', '456.0', '7890', '12345'],
            'other_col': ['A', 'B', 'C', 'D']
        })
        
        # Expected result
        expected = pd.DataFrame({
            'customer_id': ['0000000123', '0000000456', '0000007890', '0000012345'],
            'other_col': ['A', 'B', 'C', 'D']
        })
        
        # Test function
        result = pad_customer_id(df, 'customer_id', 10)
        
        # Assertions
        pd.testing.assert_frame_equal(result, expected)
        assert all(len(str(id)) == 10 for id in result['customer_id'])
    
    def test_calculate_age_groups(self):
        """Test age group calculation."""
        # Test data
        df = pd.DataFrame({
            'birth_date': [
                datetime(2000, 1, 1),  # 24 years old
                datetime(1990, 1, 1),  # 34 years old
                datetime(1980, 1, 1),  # 44 years old
                datetime(1970, 1, 1),  # 54 years old
                datetime(1960, 1, 1),  # 64 years old
                pd.NaT  # Missing birth date
            ]
        })
        
        # Test function
        result = calculate_age_groups(df, 'birth_date')
        
        # Assertions
        assert 'age' in result.columns
        assert 'age_group' in result.columns
        assert result['age_group'].iloc[0] == "19-30"  # 24 years old
        assert result['age_group'].iloc[1] == "31-50"  # 34 years old
        assert result['age_group'].iloc[2] == "31-50"  # 44 years old
        assert result['age_group'].iloc[3] == "51-65"  # 54 years old
        assert result['age_group'].iloc[4] == "65+"    # 64 years old
        assert result['age_group'].iloc[5] == "Unknown"  # Missing birth date
    
    def test_assign_data_sources(self):
        """Test data source assignment."""
        # Test data
        df = pd.DataFrame({
            'source_code': ['amazon123', 'google456', 'newsletter789', 'social_media', 'offline_store']
        })
        
        # Test function
        result = assign_data_sources(df, 'source_code')
        
        # Assertions
        assert 'source_name' in result.columns
        assert 'channel_type' in result.columns
        assert result['source_name'].iloc[0] == 'Amazon'
        assert result['source_name'].iloc[1] == 'Google'
        assert result['source_name'].iloc[2] == 'Newsletter'
        assert result['source_name'].iloc[3] == 'Social Media'
        assert result['source_name'].iloc[4] == 'Offline'
    
    def test_process_salutation(self):
        """Test salutation processing."""
        # Test data
        df = pd.DataFrame({
            'salutation': ['1', '2', '3', '4', '5', '6', '7', 'X', '0', '1.0']
        })
        
        # Test function
        result = process_salutation(df, 'salutation')
        
        # Assertions
        expected_salutations = ['Mr.', 'Ms.', 'Mr./Ms.', 'Company', 'Company Address', 'Miss', 'Family', 'Diverse', '', 'Mr.']
        assert result['salutation'].tolist() == expected_salutations
    
    def test_calculate_net_sales(self):
        """Test net sales calculation."""
        # Test data
        df = pd.DataFrame({
            'gross_amount': [100.0, 200.0, 300.0],
            'tax1': [19.0, 38.0, 57.0],
            'tax2': [0.0, 0.0, 0.0],
            'tax3': [0.0, 0.0, 0.0]
        })
        
        # Test function
        result = calculate_net_sales(df, 'gross_amount', 'tax1', 'tax2', 'tax3')
        
        # Assertions
        assert 'net_amount' in result.columns
        expected_net = [81.0, 162.0, 243.0]  # gross - tax1
        assert result['net_amount'].tolist() == expected_net
    
    def test_validate_data_quality(self):
        """Test data quality validation."""
        # Test data with some issues
        df = pd.DataFrame({
            'customer_id': ['123', '456', '789', '123'],  # Duplicate
            'name': ['John', 'Jane', None, 'Bob'],  # Missing value
            'age': [25, 30, 35, 40]
        })
        
        required_columns = ['customer_id', 'name', 'age']
        
        # Test function
        result = validate_data_quality(df, required_columns)
        
        # Assertions
        assert result['total_rows'] == 4
        assert result['missing_columns'] == []
        assert result['null_counts']['name'] == 1
        assert result['duplicate_rows'] == 1
    
    def test_create_customer_segments(self):
        """Test customer segmentation."""
        # Test data
        df = pd.DataFrame({
            'recency': [5, 4, 3, 2, 1],
            'frequency': [5, 4, 3, 2, 1],
            'monetary': [5, 4, 3, 2, 1]
        })
        
        # Test function
        result = create_customer_segments(df, 'recency', 'frequency', 'monetary')
        
        # Assertions
        assert 'customer_segment' in result.columns
        assert result['customer_segment'].iloc[0] == 'Champions'  # High R, F, M
        assert result['customer_segment'].iloc[4] == 'Lost Customers'  # Low R, F, M


class TestDataValidation:
    """Test cases for data validation."""
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame()
        required_columns = ['customer_id', 'name']
        
        result = validate_data_quality(df, required_columns)
        
        assert result['total_rows'] == 0
        assert result['missing_columns'] == required_columns
    
    def test_missing_required_columns(self):
        """Test validation with missing required columns."""
        df = pd.DataFrame({
            'customer_id': ['123', '456'],
            'other_col': ['A', 'B']
        })
        required_columns = ['customer_id', 'name', 'age']
        
        result = validate_data_quality(df, required_columns)
        
        assert 'name' in result['missing_columns']
        assert 'age' in result['missing_columns']
        assert 'customer_id' not in result['missing_columns']


class TestEdgeCases:
    """Test cases for edge cases and error handling."""
    
    def test_nan_values_in_customer_id(self):
        """Test handling of NaN values in customer ID."""
        df = pd.DataFrame({
            'customer_id': ['123', np.nan, '456'],
            'other_col': ['A', 'B', 'C']
        })
        
        result = pad_customer_id(df, 'customer_id', 10)
        
        # Should handle NaN values gracefully
        assert len(result) == 3
        assert result['customer_id'].iloc[1] == '0000000000'  # NaN becomes 0
    
    def test_invalid_dates(self):
        """Test handling of invalid dates in age calculation."""
        df = pd.DataFrame({
            'birth_date': [
                datetime(2000, 1, 1),
                'invalid_date',
                pd.NaT
            ]
        })
        
        result = calculate_age_groups(df, 'birth_date')
        
        # Should handle invalid dates gracefully
        assert len(result) == 3
        assert result['age_group'].iloc[2] == "Unknown"  # NaT becomes Unknown


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
