# Shoptet Permalink Extractor

This component enables you to download data from Shoptet permalinks.
**Table of contents:**

[TOC]

# Configuration

- Shop Name (shop_name) - [REQ] Name of shop
- Base URL (base_url) - [REQ] url of the shop
- Orders URL (orders_url) - [OPT] URL of permalink to orders data
- Products URL (products_url) - [OPT] URL of permalink to products data
- Customers URL (customers_url) - [OPT] URL of permalink to customers data
- Stock URL (stock_url) - [OPT] URL of permalink to stock data
- Additional data (additional_data) - [OPT]
    - Name (name) - [REQ] Name of additional data, will be used as table output name
    - URL (url) - [REQ] URL of permalink to additional data
- File charset. (src_charset) - [REQ] Determines the source file charset. All files will be converted to UTF-8.
- Source file delimiter (delimiter) - [REQ]
- Loading Options (loading_options) - [REQ]
   - Incremental output (incremental_output) [REQ] - If set to Incremental update (1), the result tables will be updated based on primary key. Full load (0) overwrites the destination table each time. NOTE: If you wish to remove deleted records, this needs to be set to Full load and the Period from attribute empty.
   - Date since (date_since) [OPT]
   - Date to (date_to) [OPT]
     - Date in YYYY-MM-DD format or dateparser string i.e. 5 days ago, 1 month ago, yesterday, etc. If left empty, all records are downloaded.
   - Backfill mode (backfill_mode) [OPT]
   - Chunk size (chunk_size_days) [OPT]

## Example configuration

```json
{
  "shop_name": "test",
  "base_url": "ss",
  "orders_url": "URL",
  "products_url": "URL",
  "customers_url": "URL",
  "stock_url": "URL",
  "additional_data": [],
  "src_charset": "windows-1250",
  "delimiter": ";",
  "loading_options": {
    "date_to": "now",
    "date_since": "1 week ago",
    "backfill_mode": 0,
    "chunk_size_days": 360,
    "incremental_output": 1
  }
}
```