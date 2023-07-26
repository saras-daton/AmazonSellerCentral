# Amazon Seller Partner Data Unification

This dbt package is for the Amazon Selling Partner data unification Ingested by [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

### Supported Datawarehouses:
- BigQuery
- Snowflake

#### Typical challenges with raw data are:
- Array/Nested Array columns which makes queries for Data Analytics complex
- Data duplication due to look back period while fetching report data from Amazon Seller Partner
- Separate tables at marketplaces/Store, brand, account level for same kind of report/data feeds

By doing Data Unification the above challenges can be overcomed and simplifies Data Analytics. 
As part of Data Unification, the following functions are performed:
- Consolidation - Different marketplaces/Store/account & different brands would have similar raw Daton Ingested tables, which are consolidated into one table with column distinguishers brand & store
- Deduplication - Based on primary keys, the data is De-duplicated and the latest records are only loaded into the consolidated stage tables
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- Standardization -
	- Currency Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local currency of the corresponding marketplace/store/account. Values that are in local currency are standardized by converting to desired currency using Daton Exchange Rates data.
	  Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this](https://github.com/saras-daton/currency_exchange_rates)
	- Time Zone Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local timezone of the corresponding marketplace/store/account. DateTime values that are in local timezone are standardized by converting to specified timezone using input offset hours.

#### Prerequisite 
Daton Integrations for  
- Amazon Seller Partner 
- Exchange Rates(Optional, if currency conversion is not required)

# Configuration 

## Required Variables

This package assumes that you have an existing dbt project with a BigQuery/Snowflake profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.
```yaml
vars:
    raw_database: "your_database"
    raw_schema: "your_schema"
```

## Setting Target Schema

Models will be create unified tables under the schema (<target_schema>_stg_amazon). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yaml
models:
  amazon_sellerpartner:
    +schema: custom_schema_extension
```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

### Currency Conversion 

To enable currency conversion, which produces two columns - exchange_currency_rate & exchange_currency_code, please mark the currency_conversion_flag as True. By default, it is False.
Prerequisite - Daton Exchange Rates Integration

Example:
```yaml
vars:
    currency_conversion_flag: True
```

### Timezone Conversion 

To enable timezone conversion, which converts the timezone columns from UTC timezone to local timezone, please mark the timezone_conversion_flag as True in the dbt_project.yml file, by default, it is False
Additionally, you need to provide offset hours between UTC and the timezone you want the data to convert into for each raw table

Example:
```yaml
vars:
timezone_conversion_flag: False
raw_table_timezone_offset_hours: {
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_FlatFileAllOrdersReportbyLastUpdate":-7,
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_ListOrder":-7,
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_FBAAmazonFulfilledShipmentsReport":-7,
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_InventoryLedgerDetailedReport":-7
    }
```
Here, -7 represents the offset hours between UTC and PDT considering we are sitting in PDT timezone and want the data in this timezone

### Table Exclusions

If you need to exclude any of the models, declare the model names as variables and mark them as False. Refer the table below for model details. By default, all tables are created.

Example:
```yaml
vars:
ListOrder: False
```

## Models

This package contains models from the Amazon Selling Partner API which includes reports on {{sales, margin, inventory, product}}. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Customer | [ListOrder](models/Customer/ListOrder.sql)  | A list orders along with the customer details |
|Inventory | [FBAManageInventoryHealthReport](models/AmazonSellerCentral/FBAManageInventoryHealthReport.sql)  | A detailed report which gives details about inventory age , current inventory levels, recommended inventory levels |
|Inventory | [FBAManageInventory](models/AmazonSellerCentral/FBAManageInventory.sql)  | A list of ad groups associated with the accountA report which gives details about inventory movement - inbound, outbound, sellable |
|Inventory | [InventoryLedgerDetailedReport](models/AmazonSellerCentral/InventoryLedgerDetailedReport.sql)| A report about available quantity at the warehouse level |
|Financial Events | [ListFinancialEventsOrderFees](models/AmazonSellerCentral/ListFinancialEventsOrderFees.sql)| A list of fees associated with the shipment item. |
|Financial Events | [ListFinancialEventsOrderPromotions](models/AmazonSellerCentral/ListFinancialEventsOrderPromotions.sql)| A list of promotions which gives the amount of promotional discount applied to the item at an item & order level.|
|Financial Events | [ListFinancialEventsOrderRevenue](models/AmazonSellerCentral/ListFinancialEventsOrderRevenue.sql)| A list of shipment items which includes order & product level revenue |
|Financial Events | [ListFinancialEventsOrderTaxes](models/AmazonSellerCentral/ListFinancialEventsOrderTaxes.sql)| A list of order taxes |
|Financial Events | [ListFinancialEventsRefundFees](models/AmazonSellerCentral/ListFinancialEventsRefundFees.sql)| A list of fees associated with the refunded item.	 |
|Financial Events | [ListFinancialEventsRefundPromotions](models/AmazonSellerCentral/ListFinancialEventsRefundPromotions.sql)|A list of promotions which gives the amount of promotional discount applied to the item at an refunded item level. |
|Financial Events | [ListFinancialEventsRefundRevenue](models/AmazonSellerCentral/ListFinancialEventsRefundRevenue.sql)| A list of refunded items which includes refund & product level revenue |
|Financial Events | [ListFinancialEventsRefundTaxes](models/AmazonSellerCentral/ListFinancialEventsRefundTaxes.sql)| A list of refund taxes |
|Financial Events | [ListFinancialEventsServicefees](models/AmazonSellerCentral/ListFinancialEventsServicefees.sql)| A list of service level fees |
|Product | [CatalogItemsSummary](models/AmazonSellerCentral/CatalogItemsSummary.sql)| A list of product summary, manufacturer & dimensions |
|Product | [AllListingsReport](models/AmazonSellerCentral/AllListingsReport.sql)|  listing report with details about all types of listings |
|Returns | [FBAReturnsReport](models/AmazonSellerCentral/FBAReturnsReport.sql)|Returns report of the orders fulfilled by Amazon |
|Returns | [FlatFileReturnsReportByReturnDate](models/AmazonSellerCentral/FlatFileReturnsReportByReturnDate.sql)|Returns report of the orders fulfilled by Merchant |
|Sales | [FBAAmazonFulfilledShipmentsReport](models/AmazonSellerCentral/FBAAmazonFulfilledShipmentsReport.sql)|Orders report with shipment details included |
|Sales | [FlatFileAllOrdersReportByLastUpdate](models/AmazonSellerCentral/FlatFileAllOrdersReportByLastUpdate.sql)|Order & Item Level report |
|Sales | [SalesAndTrafficReportByChildASIN](models/AmazonSellerCentral/SalesAndTrafficReportByChildASIN.sql)|Provides sales & traffic at SKU level that we see in the Business Report in the UI |


## DBT Tests

The tests property defines assertions about a column, table, or view. The property contains a list of generic tests, referenced by name, which can include the four built-in generic tests available in dbt. For example, you can add tests that ensure a column contains no duplicates and zero null values. Any arguments or configurations passed to those tests should be nested below the test name.

| **Tests**  | **Description** |
| ---------------| ------------------------------------------- |
| [Not Null Test](https://docs.getdbt.com/reference/resource-properties/tests#testing-an-expression)  | This test validates that there are no null values present in a column |
| [Data Recency Test](https://github.com/dbt-labs/dbt-utils/blob/main/macros/generic_tests/recency.sql)  | This is used to check for issues with data refresh within 1 day |
| [Accepted Value Test](https://docs.getdbt.com/reference/resource-properties/tests#accepted_values)  | This test validates that all of the values in a column are present in a supplied list of values. If any values other than those provided in the list are present, then the test will fail, by default it consists of default values and this needs to be changed based on the project |
| [Uniqueness Test](https://docs.getdbt.com/reference/resource-properties/tests#testing-an-expression)  | This test validates that there are no duplicate values present in a field |

### Table Name: ListOrder

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| PurchaseDate | Yes | Yes |  | Yes |
| amazonorderid | Yes |  |  | Yes |
| marketplaceName |  |  |  | Yes |
| brand |  |  | Yes |  |

### Table Name: FBAManageInventoryHealthReport

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| snapshot_date | Yes | Yes |  | Yes |
| asin | Yes |  |  | Yes |
| sku | Yes |  |  | Yes |
| marketplaceId |  |  |  | Yes |
| brand |  |  | Yes |  |


### Table Name: FBAManageInventory

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| ReportstartDate | Yes | Yes |  | Yes |
| sku | Yes |  |  | Yes |
| marketplaceId |  |  |  | Yes |
| brand |  |  | Yes |  |

### Table Name: InventoryLedgerDetailedReport

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| date | Yes | Yes |  | Yes |
| asin | Yes |  |  | Yes |
| fulfillment_center | Yes |  |  | Yes |
| msku | Yes |  |  | Yes |
| event_type | Yes |  |  | Yes |
| reference_id | Yes |  |  | Yes |
| quantity | Yes |  |  |  |
| disposition | Yes |  |  | Yes |
| marketplaceid |  |  |  | Yes |
| brand |  |  | Yes |  |

### Table Name: ListFinancialEventsOrderFees

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| posteddate | Yes | Yes |  | Yes |
| marketplacename |  |  |  | Yes |
| amazonorderid | Yes |  |  | Yes |
| FeeType | Yes |  |  | Yes |
| _seq_id |  |  |  | Yes |
| brand |  |  | Yes |  |

### Table Name: ListFinancialEventsOrderPromotions

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| posteddate | Yes | Yes |  | Yes |
| marketplacename |  |  |  | Yes |
| amazonorderid |  |  |  | Yes |
| PromotionType | Yes |  |  | Yes |
| _seq_id |  |  |  | Yes |
| brand |  |  | Yes |  |


### Table Name: ListFinancialEventsOrderRevenue

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| posteddate | Yes | Yes |  | Yes |
| marketplacename |  |  |  | Yes |
| amazonorderid | Yes |  |  | Yes |
| ChargeType | Yes |  |  | Yes |
| _seq_id |  |  |  | Yes |
| brand |  |  | Yes |  |

### Table Name: ListFinancialEventsOrderTaxes

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| posteddate | Yes | Yes |  | Yes |
| marketplacename |  |  |  | Yes |
| amazonorderid |  |  |  | Yes |
| ChargeType |  |  |  | Yes |
| _seq_id |  |  |  | Yes |
| brand |  |  | Yes |  |


### Table Name: ListFinancialEventsRefundFees

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| posteddate | Yes | Yes |  | Yes |
| marketplacename |  |  |  | Yes |
| amazonorderid |  |  |  | Yes |
| FeeType |  |  |  | Yes |
| _seq_id |  |  |  | Yes |
| brand |  |  | Yes |  |


### Table Name: ListFinancialEventsRefundPromotions

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| posteddate | Yes | Yes |  | Yes |
| marketplacename |  |  |  | Yes |
| amazonorderid |  |  |  | Yes |
| PromotionType |  |  |  | Yes |
| _seq_id |  |  |  | Yes |
| brand |  |  | Yes |  |

### Table Name: ListFinancialEventsRefundRevenue

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| posteddate | Yes | Yes |  | Yes |
| marketplacename |  |  |  | Yes |
| amazonorderid |  |  |  | Yes |
| ChargeType |  |  |  | Yes |
| _seq_id |  |  |  | Yes |
| brand |  |  | Yes |  |


### Table Name: ListFinancialEventsRefundTaxes

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| posteddate | Yes | Yes |  | Yes |
| marketplacename |  |  |  | Yes |
| amazonorderid |  |  |  | Yes |
| ChargeType |  |  |  | Yes |
| _seq_id |  |  |  | Yes |
| brand |  |  | Yes |  |

### Table Name: CatalogItems

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| brandName |  |  |  | Yes |
| ReferenceASIN | Yes |  |  | Yes |
| merchant_sku | Yes |  |  | Yes |
| modelNumber |  |  |  | Yes |
| marketplaceId |  |  |  | Yes |
| RequestStartDate | Yes |  |  |  |

### Table Name: AllListingsReport

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| seller_sku | Yes |  |  | Yes |
| listing_id | Yes |  |  | Yes |


### Table Name: FBAReturnsReport

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| return_date | Yes | Yes |  | Yes |
| asin | Yes |  |  | Yes |
| sku | Yes |  |  | Yes |
| order_id | Yes |  |  | Yes |
| fnsku | Yes |  |  | Yes |
| license_plate_number | Yes |  |  | Yes |
| fulfillment_center_id | Yes |  |  | Yes |
| _seq_id |  |  |  | Yes |
| marketplaceId |  |  |  | Yes |
| brand |  |  | Yes |  |

### Table Name: FlatFileReturnsReportByReturnDate

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| Return_request_date | Yes | Yes |  | Yes |
| Order_ID | Yes |  |  | Yes |
| ASIN | Yes |  |  | Yes |
| marketplaceId |  |  |  | Yes |
| brand |  |  | Yes |  |

### Table Name: FBAAmazonFulfilledShipmentsReport

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| purchase_date | Yes | Yes |  | Yes |
| sku | Yes |  |  | Yes |
| amazon_order_id | Yes |  |  | Yes |
| marketplaceName |  |  |  | Yes |
| _seq_id |  |  |  | Yes |
| brand |  |  | Yes |  |


### Table Name: FlatFileAllOrdersReportByLastUpdate

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| purchase_date | Yes | Yes |  | Yes |
| amazon_order_id | Yes |  |  | Yes |
| asin | Yes |  |  | Yes |
| sku | Yes |  |  | Yes |
| _seq_id |  |  |  | Yes |
| brand |  |  | Yes |  |


### Table Name: SalesAndTrafficReportByChildASIN

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| :--------------- | :--------------- | :--------------- | :--------------- | :--------------- |
| date | Yes | Yes |  | Yes |
| parentAsin | Yes |  |  | Yes |
| childAsin | Yes |  |  | Yes |
| sku |  |  |  | Yes |
| marketplaceId |  |  |  | Yes |
| brand |  |  | Yes |  |






### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package as below. 
	You can overwrite these default configurations by using your project specific properties yaml.
```yaml
version: 2
models:
  - name: ListOrder
    description: A list orders along with the customer details
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['PurchaseDate','amazonorderid']
      partition_by: { 'field': 'PurchaseDate', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['amazonorderid']

  - name: FBAManageInventoryHealthReport  
    description: A detailed report which gives details about inventory age , current inventory levels, recommended inventory levels
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['snapshot_date','asin','sku']
      partition_by: { 'field': 'snapshot_date', 'data_type': 'date' }
      cluster_by: ['asin','sku']

  - name: FBAManageInventory
    description: A list of ad groups associated with the accountA report which gives details about inventory movement - inbound, outbound, sellable
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['ReportstartDate','sku']
      partition_by: { 'field': 'ReportstartDate', 'data_type': 'date' }
      cluster_by: ['sku']

  - name: InventoryLedgerDetailedReport
    description: A report about available quantity at the warehouse level
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['date','asin','fulfillment_center','msku', 'event_type', 'reference_id','quantity','disposition']
      partition_by: { 'field': 'date', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['date','msku']

  - name: ListFinancialEventsOrderFees
    description: A list of fees associated with the shipment item.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['posteddate', 'marketplacename', 'amazonorderid', 'FeeType', 'TransactionType', 'AmountType','_seq_id']
      partition_by: { 'field': 'posteddate', 'data_type': 'date' }
      cluster_by: ['marketplacename', 'amazonorderid']

  - name: ListFinancialEventsOrderPromotions
    description: A list of promotions which gives the amount of promotional discount applied to the item at an item & order level.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['posteddate', 'marketplacename', 'amazonorderid', 'PromotionType', 'TransactionType', 'AmountType','_seq_id']
      partition_by: { 'field': 'posteddate', 'data_type': 'date' }
      cluster_by: ['marketplacename', 'amazonorderid']

  - name: ListFinancialEventsOrderRevenue
    description: A list of shipment items which includes order & product level revenue
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['posteddate', 'marketplacename', 'amazonorderid', 'ChargeType', 'TransactionType', 'AmountType', '_seq_id']
      partition_by: { 'field': 'posteddate', 'data_type': 'date' }
      cluster_by: ['marketplacename', 'amazonorderid']

  - name: ListFinancialEventsOrderTaxes
    description: A list of order taxes
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['posteddate', 'marketplacename', 'amazonorderid', 'ChargeType', 'TransactionType', 'AmountType', '_seq_id']
      partition_by: { 'field': 'posteddate', 'data_type': 'date' }
      cluster_by: ['marketplacename', 'amazonorderid']

  - name: ListFinancialEventsRefundFees
    description: A list of fees associated with the refunded item.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['posteddate', 'marketplacename', 'amazonorderid', 'FeeType', '_seq_id']
      partition_by: { 'field': 'posteddate', 'data_type': 'date' }
      cluster_by: ['marketplacename', 'amazonorderid']

  - name: ListFinancialEventsRefundPromotions
    description: A list of promotions which gives the amount of promotional discount applied to the item at an refunded item level.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['posteddate', 'marketplacename', 'amazonorderid', 'PromotionType', '_seq_id']
      partition_by: { 'field': 'posteddate', 'data_type': 'date' }
      cluster_by: ['marketplacename', 'amazonorderid']

  - name: ListFinancialEventsRefundRevenue
    description: A list of refunded items which includes refund & product level revenue
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['posteddate', 'marketplacename', 'amazonorderid', 'ChargeType', '_seq_id']
      partition_by: { 'field': 'posteddate', 'data_type': 'date' }
      cluster_by: ['marketplacename', 'amazonorderid']

  - name: ListFinancialEventsRefundTaxes
    description: A list of refund taxes
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['posteddate', 'marketplacename', 'amazonorderid', 'ChargeType', '_seq_id']
      partition_by: { 'field': 'posteddate', 'data_type': 'date' }
      cluster_by: ['marketplacename', 'amazonorderid']

  - name: CatalogItemsSummary
    description: A list of product summary, manufacturer & dimensions
    config:
      materialized: incremental
      incremental_strategy: merge
      cluster_by: ['ReferenceASIN']
      unique_key: ['brandName','ReferenceASIN','modelNumber']

  - name: AllListingsReport
    description: A listing report with details about all types of listings
    config:
      materialized: incremental
      incremental_strategy: merge
      cluster_by: ['seller_sku']
      unique_key: ['seller_sku']

  - name: FBAReturnsReport
    description: Returns report of the orders fulfilled by Amazon
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['return_date','asin','sku','order_id','fnsku','license_plate_number','fulfillment_center_id','_seq_id']
      partition_by: { 'field': 'return_date', 'data_type': 'date' }
      cluster_by: ['asin','sku']

  - name: FlatFileReturnsReportByReturnDate
    description: Returns report of the orders fulfilled by Merchant
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['Return_request_date', 'Order_ID', 'ASIN']
      partition_by: { 'field': 'Return_request_date', 'data_type': 'date' }
      cluster_by: ['ASIN','Merchant_SKU', 'Order_ID']

  - name: FBAAmazonFulfilledShipmentsReport
    description: Orders report with shipment details included
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['purchase_date', 'sku', 'amazon_order_id', '_seq_id']
      partition_by: { 'field': 'purchase_date', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['sku','amazon_order_id']

  - name: FlatFileAllOrdersReportByLastUpdate
    description: Order & Item Level report
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['purchase_date', 'amazon_order_id', 'asin', 'sku', '_seq_id']
      partition_by: { 'field': 'purchase_date', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['asin', 'sku', 'amazon_order_id']

  - name: SalesAndTrafficReportByChildASIN
    description: Provides sales & traffic at SKU level that we see in the Business Report in the UI
    config:
      materialized: incremental
      incremental_strategy: merge
      partition_by: { 'field': 'date', 'data_type': 'date' }
      cluster_by: ['parentAsin', 'childAsin']
      unique_key: ['date', 'parentAsin', 'childAsin']
```



## Resources:
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}



