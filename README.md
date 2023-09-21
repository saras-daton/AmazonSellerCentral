# Amazon Seller Partner Data Unification

## What is the purpose of this dbt package?
This dbt package is for the Amazon Selling Partner data unification ingested by Daton. Daton is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com/).

## How do I use Amazon Seller Partner dbt package?

### Supported Datawarehouses:
- [BigQuery](https://sarasanalytics.com/blog/what-is-google-bigquery/)
- [Snowflake](https://sarasanalytics.com/daton/snowflake/)

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

#### Prerequisite for Amazon Seller Partner dbt package
Daton Integrations for  
- [Amazon Seller Partner](https://sarasanalytics.com/blog/amazon-seller-central/) 
- Exchange Rates(Optional, if currency conversion is not required)

# Configuration for dbt package 

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

| **Category**                 | **Model**  | **Description** | **Unique Key** | **Partition Key** | **Cluster Key** |
| ----------------- | ---------------| ----------------------- | ------------ | ---------- | ------------ |
|Customer | [ListOrder](models/AmazonSellerCentral/ListOrder.sql)  | A list orders along with the customer details |  PurchaseDate, <br /> amazonorderid, <br /> marketplaceName, <br /> sellingPartnerId | PurchaseDate  | LastUpdateDate, <br /> PurchaseDate, <br /> amazonorderid |
|Inventory | [FBAManageInventoryHealthReport](models/AmazonSellerCentral/FBAManageInventoryHealthReport.sql)  | A detailed report which gives details about inventory age, current inventory levels, recommended inventory levels | snapshot_date, <br /> asin, <br /> sku, <br /> marketplaceId, <br /> sellingPartnerId | snapshot_date  | snapshot_date, <br /> asin, <br /> sku |
|Inventory | [FBAManageInventory](models/AmazonSellerCentral/FBAManageInventory.sql)  | A report which gives details about inventory movement - inbound, outbound, sellable | ReportstartDate, <br /> sku, <br /> marketplaceName | ReportstartDate | ReportstartDate, <br /> sku |
|Inventory | [InventoryLedgerDetailedReport](models/AmazonSellerCentral/InventoryLedgerDetailedReport.sql)| A report about available quantity at the warehouse level | date, <br /> asin, <br /> fulfillment_center, <br /> msku, <br /> event_type, <br /> reference_id, <br /> quantity, <br /> disposition, <br /> marketplaceId, <br /> sellingPartnerId  | date | date, <br /> asin, <br /> msku |
|Financial Events | [ListFinancialEventsOrderFees](models/AmazonSellerCentral/ListFinancialEventsOrderFees.sql)| A list of fees associated with the shipment item. | ShipmentEventlist_PostedDate, <br /> ShipmentEventlist_MarketplaceName, <br /> ShipmentEventlist_AmazonOrderId, <br /> ItemFeeList_FeeType, <br /> _seq_id | ShipmentEventlist_PostedDate | ShipmentEventlist_PostedDate, <br /> ShipmentEventlist_MarketplaceName, <br /> ShipmentEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsOrderPromotions](models/AmazonSellerCentral/ListFinancialEventsOrderPromotions.sql)| A list of promotions which gives the amount of promotional discount applied to the item at an item & order level.|  ShipmentEventlist_PostedDate, <br /> ShipmentEventlist_MarketplaceName, <br /> ShipmentEventlist_AmazonOrderId, <br /> PromotionList_PromotionType, <br /> _seq_id | ShipmentEventlist_PostedDate | ShipmentEventlist_PostedDate, <br /> ShipmentEventlist_MarketplaceName, <br /> ShipmentEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsOrderRevenue](models/AmazonSellerCentral/ListFinancialEventsOrderRevenue.sql)| A list of shipment items which includes order & product level revenue | ShipmentEventlist_PostedDate, <br /> ShipmentEventlist_MarketplaceName, <br /> ShipmentEventlist_AmazonOrderId, <br /> ItemChargeList_ChargeType, <br /> _seq_id | ShipmentEventlist_PostedDate | ShipmentEventlist_PostedDate, <br /> ShipmentEventlist_MarketplaceName, <br /> ShipmentEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsOrderTaxes](models/AmazonSellerCentral/ListFinancialEventsOrderTaxes.sql)| A list of order taxes | ShipmentEventlist_PostedDate, <br /> ShipmentEventlist_MarketplaceName, <br /> ShipmentEventlist_AmazonOrderId, <br /> TaxesWithheld_ChargeType, <br /> _seq_id | ShipmentEventlist_PostedDate | ShipmentEventlist_PostedDate, <br /> ShipmentEventlist_MarketplaceName, <br /> ShipmentEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsRefundFees](models/AmazonSellerCentral/ListFinancialEventsRefundFees.sql)| A list of fees associated with the refunded item.	 | RefundEventlist_PostedDate, <br /> RefundEventlist_MarketplaceName, <br /> RefundEventlist_AmazonOrderId, <br /> ItemFeeAdjustmentList_FeeType, <br /> _seq_id | RefundEventlist_PostedDate | RefundEventlist_PostedDate, <br /> RefundEventlist_MarketplaceName, <br /> RefundEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsRefundPromotions](models/AmazonSellerCentral/ListFinancialEventsRefundPromotions.sql)|A list of promotions which gives the amount of promotional discount applied to the item at an refunded item level. | RefundEventlist_PostedDate, <br /> RefundEventlist_MarketplaceName, <br /> RefundEventlist_AmazonOrderId, <br /> PromotionList_PromotionType, <br /> _seq_id | RefundEventlist_PostedDate | RefundEventlist_PostedDate, <br /> RefundEventlist_MarketplaceName, <br /> RefundEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsRefundRevenue](models/AmazonSellerCentral/ListFinancialEventsRefundRevenue.sql)| A list of refunded items which includes refund & product level revenue | RefundEventlist_PostedDate, <br /> RefundEventlist_MarketplaceName, <br /> RefundEventlist_AmazonOrderId, <br /> ItemChargeAdjustmentList_ChargeType, <br /> _seq_id | RefundEventlist_PostedDate | RefundEventlist_PostedDate, <br /> RefundEventlist_MarketplaceName, <br /> RefundEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsRefundTaxes](models/AmazonSellerCentral/ListFinancialEventsRefundTaxes.sql)| A list of refund taxes | RefundEventlist_PostedDate, <br /> RefundEventlist_MarketplaceName, <br /> RefundEventlist_AmazonOrderId, <br /> TaxesWithheld_ChargeType, <br /> _seq_id | RefundEventlist_PostedDate | RefundEventlist_PostedDate, <br /> RefundEventlist_MarketplaceName, <br /> RefundEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsServicefees](models/AmazonSellerCentral/ListFinancialEventsServicefees.sql)| A list of service level fees | RequestStartDate, <br /> marketplaceId, <br /> ServiceFeeEventList_FeeReason, <br /> FeeList_FeeType, <br /> ServiceFeeEventList_SellerSKU, <br /> ServiceFeeEventList_FeeDescription, <br /> _seq_id | RequestStartDate | RequestStartDate, <br /> marketplaceName, <br /> sellingPartnerId, <br /> amazonorderid |
|Product | [CatalogItems](models/AmazonSellerCentral/CatalogItems.sql)| A list of product summary, manufacturer & dimensions | summaries_brandName, <br /> ReferenceASIN, <br /> summaries_modelNumber, <br /> marketplaceId, <br /> sellingPartnerId | RequestStartDate | RequestStartDate, <br /> summaries_brandName, <br /> ReferenceASIN |
|Product | [AllListingsReport](models/AmazonSellerCentral/AllListingsReport.sql)| A listing report with details about all types of listings | ReportstartDate, <br /> seller_sku | | seller_sku, <br /> listing_id |
|Returns | [FBAReturnsReport](models/AmazonSellerCentral/FBAReturnsReport.sql)|Returns report of the orders fulfilled by Amazon | return_date, <br /> asin, <br /> sku, <br /> order_id, <br /> fnsku, <br /> license_plate_number, <br /> fulfillment_center_id, <br /> marketplaceId, <br /> _seq_id | return_date | ReportstartDate, <br /> return_date, <br /> asin, <br /> sku |
|Returns | [FlatFileReturnsReportByReturnDate](models/AmazonSellerCentral/FlatFileReturnsReportByReturnDate.sql)|Returns report of the orders fulfilled by Merchant | Return_request_date, <br /> Order_ID, <br /> ASIN, <br /> marketplaceId | Return_request_date | ReportstartDate, <br /> Return_request_date, <br /> ASIN, <br /> Order_ID |
|Sales | [FBAAmazonFulfilledShipmentsReport](models/AmazonSellerCentral/FBAAmazonFulfilledShipmentsReport.sql)|Orders report with shipment details included | purchase_date, <br /> sku, <br /> amazon_order_id, <br /> marketplaceName, <br /> _seq_id | purchase_date | reporting_date, <br /> purchase_date, <br /> sku, <br /> amazon_order_id | 
|Sales | [FlatFileAllOrdersReportByLastUpdate](models/AmazonSellerCentral/FlatFileAllOrdersReportByLastUpdate.sql)|Order & Item Level report | purchase_date, <br /> amazon_order_id, <br /> asin, <br /> sku, <br /> _seq_id | purchase_date | last_updated_date, <br /> purchase_date, <br /> asin, <br /> amazon_order_id |
|Sales | [SalesAndTrafficReportByChildASIN](models/AmazonSellerCentral/SalesAndTrafficReportByChildASIN.sql)|Provides sales & traffic at SKU level that we see in the Business Report in the UI | date, <br /> parentAsin, <br /> childAsin, <br /> marketplaceId, <br /> sellingPartnerId | date | ReportstartDate, <br /> date, <br /> parentAsin, <br /> childAsin |


### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package.

## Resources:
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}
