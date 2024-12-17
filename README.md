# Shopify Order Backfill Script for Segment

## Overview
This script processes historical Shopify order data from CSV files and sends it to Segment as tracked events. It's designed to handle bulk order data while maintaining data integrity and providing detailed processing metrics.

## Setup

1. Install dependencies:

npm install @segment/analytics-node csv-parser json2csv

2. Configure the script:

const backfillFile = '' // Replace with your CSV file path

const segmentWriteKey = '' // Replace with your Segment write key


3. Place your Shopify CSV export files in a `CSVs` directory in the project folder

## Usage

Run the script:

node index.js


## Core Functionality

### Primary Purpose
- Reads Shopify order data from CSV files
- Transforms order data into Segment-compatible format
- Sends orders to Segment as "Order Completed" events with full order details
- Maintains user identity by hashing email addresses (MD5) for consistency

### Order Processing Features
- **Multi-Product Order Handling**: Consolidates multiple line items into single orders
- **Order Status Filtering**: Only processes paid orders, skipping other order statuses
- **Product Details**: Captures comprehensive product information including:
  - SKU
  - Price
  - Quantity
  - Product name
  - Line item details

### Data Validation & Quality
- **Email Validation**: Filters out invalid email addresses
- **Date Validation**: Ensures order timestamps are valid
- **Financial Status Checking**: Processes only 'paid' orders
- **Unique User Tracking**: Maintains count of unique customers processed

### Error Handling & Logging
- **Error Row Capture**: Saves failed records to `error_rows.csv`
- **Progress Monitoring**: Logs progress every 1000 records
- **Detailed Summary Statistics**:
  - Total rows processed
  - Records sent to Segment
  - Records skipped
  - Unique users count
  - Invalid email count

### Technical Features
- **Batch Processing Control**: Optional record limit for testing (`breakAfter`)
- **CSV Header Handling**: Maintains original header format
- **Efficient Data Structures**: Uses Sets for unique email tracking
- **Asynchronous Processing**: Handles data streaming and API calls asynchronously

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `backfillFile` | CSV file name in CSVs directory | Required |
| `breakAfter` | Number of records to process (for testing) | 100 |
| `segmentWriteKey` | Segment Write Key | Required |

## Output Metrics
The script provides comprehensive processing statistics including:
- Total records processed
- Successfully sent records
- Skipped records
- Unique user count
- Invalid email count
- Error logs for failed records

## Error Recovery
- Failed records are automatically logged to CSV
- Maintains processing state for debugging
- Detailed error messaging for troubleshooting

## File Structure

```
├── README.md
├── index.js
├── CSVs/
│ └── [your-shopify-export.csv]
└── error_rows.csv (generated)
```


## Required CSV Headers
The script expects Shopify order export format with the following key headers:
- Email
- Financial Status
- Created at
- Id
- Billing Name
- Billing Country
- Currency
- Lineitem price
- Lineitem sku
- Lineitem quantity
- Lineitem name

## Contributing
Feel free to submit issues and enhancement requests.