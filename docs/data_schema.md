# Data Schema Documentation

This document describes the expected data schema for the ETL pipeline.

## Customer Addresses (customer_addresses.csv)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| customer_id | string | Unique customer identifier | "0000001234" |
| salutation | string | Customer salutation code | "1", "2", "3" |
| title | string | Customer title | "Dr.", "Prof." |
| first_name | string | Customer first name | "John" |
| last_name | string | Customer last name | "Doe" |
| source | string | Customer acquisition source | "google", "newsletter" |
| postal_code | string | Customer postal code | "12345" |
| city | string | Customer city | "Berlin" |
| marketing_consent | string | Marketing consent flag | "Y", "N" |
| phone | string | Customer phone number | "+49123456789" |
| registration_date | date | Customer registration date | "2023-01-15" |
| birth_date | date | Customer birth date | "1985-05-20" |

## Customer Transactions (customer_transactions.csv)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| invoice_number | string | Invoice number | "INV123456" |
| order_number | string | Order number | "ORD789012" |
| customer_reference | string | Customer reference code | "XX0000001234" |
| transaction_date | date | Transaction date | "2023-06-15" |
| gross_amount | float | Gross transaction amount | 150.00 |
| tax1 | float | First tax amount | 19.00 |
| tax2 | float | Second tax amount | 0.00 |
| tax3 | float | Third tax amount | 0.00 |

## Customer Emails (customer_emails.csv)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| customer_id | string | Customer identifier | "0000001234" |
| email_address | string | Customer email address | "john.doe@example.com" |
| is_primary | string | Primary email flag | "Y", "N" |

## Advertising Data (advertising_data.csv)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| customer_id | string | Customer identifier | "0000001234" |
| advertising_date | date | Advertising contact date | "2023-06-10" |
| media_code | string | Media channel code | "newsletter", "social" |

## Email Preferences (email_preferences.csv)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| customer_id | string | Customer identifier | "0000001234" |
| email_type | string | Email preference type | "newsletter", "promotional" |

## Customer Groups (customer_groups_{country}.csv)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| customer_id | string | Customer identifier | "0000001234" |
| customer_group | string | Customer group classification | "Champions", "Loyal Customers" |

## Output Files

### Customer Analysis Output

| Column | Type | Description |
|--------|------|-------------|
| customer_id | string | Customer identifier |
| salutation | string | Processed salutation |
| first_name | string | Customer first name |
| last_name | string | Customer last name |
| age_group | string | Age group classification |
| city | string | Customer city |
| order_id | string | Order identifier |
| order_date | date | Order date |
| total_net_sales | float | Total net sales amount |
| source_name | string | Customer acquisition source |
| channel_type | string | Channel type (Online/Offline) |
| last_advertising_date | date | Last advertising contact |
| last_advertising_media | string | Last advertising media |
| country | string | Country code |

### RFM Analysis Output

| Column | Type | Description |
|--------|------|-------------|
| customer_id | string | Customer identifier |
| email_type | string | Email preference type |
| registration_date | date | Customer registration date |
| frequency | int | Total order frequency |
| monetary | float | Total monetary value |
| recency | date | Last transaction date |
| freq_3to5_years | int | Frequency 3-5 years ago |
| monetary_3to5_years | float | Monetary value 3-5 years ago |
| freq_last_2_years | int | Frequency last 2 years |
| monetary_last_2_years | float | Monetary value last 2 years |
| freq_5_years | int | Weighted 5-year frequency |
| monetary_5_years | float | Weighted 5-year monetary |
| recency_days | int | Days since last transaction |
| r_score | int | Recency score (1-5) |
| f_score | int | Frequency score (1-5) |
| m_score | int | Monetary score (1-5) |
| mf_score | float | Combined monetary-frequency score |
| customer_segment | string | RFM customer segment |
| customer_group | string | Customer group classification |

## Data Quality Requirements

### Required Columns
All input files must contain the specified columns with appropriate data types.

### Data Validation Rules
- **Customer IDs**: Must be 10-digit strings, padded with leading zeros
- **Dates**: Must be in YYYY-MM-DD format
- **Amounts**: Must be numeric values (float)
- **Codes**: Must follow specified code formats

### Missing Data Handling
- Missing customer IDs are not allowed
- Missing dates are converted to NaT (Not a Time)
- Missing amounts are treated as 0.0
- Missing strings are converted to empty strings

## File Naming Conventions

### Input Files
- Country-specific folders: `F01/`, `F02/`, `F03/`, `F04/`
- Standard file names within each country folder
- CSV format with semicolon separator
- CP850 encoding

### Output Files
- `customer_analysis_{country}.csv`
- `rfm_segments_{country}.csv`
- `customer_groups_{country}.csv`

## Data Processing Notes

### Customer ID Processing
- All customer IDs are standardized to 10-digit format
- Leading zeros are added as needed
- ".0" suffixes are removed

### Date Processing
- All dates are parsed as datetime objects
- Invalid dates are converted to NaT
- Date comparisons use pandas Timestamp objects

### Amount Processing
- All monetary values are converted to float
- Net amounts are calculated by subtracting taxes from gross amounts
- Negative amounts are handled appropriately

### String Processing
- All string operations use pandas string methods
- Missing values are handled gracefully
- Case-insensitive matching where appropriate
