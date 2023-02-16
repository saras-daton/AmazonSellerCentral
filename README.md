Amazon Seller Partner Data Unification
This dbt package is for the Amazon Selling Partner data unification Ingested by [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

Supported Datawarehouses:
- BigQuery
- Snowflake

Typical challanges with raw data are:
- Array/Nested Array columns which makes queries for Data Analytics complex
- Data duplication due to look back period while fetching report data from Amazon Seller Partner
- Seperate tables at marketplaces/Store, brand, account level for same kind of report/data feeds

By doing Data Unification the above challenges can be overcomed and simplifies Data Analytics. 
As part of Data Unification, the following funtions are performed:
- Consolidation - Different marketplaces/Store/account & different brands would have similar raw Daton Ingested tables, which are consolidated into one table with column distinguishers brand & store
- Deduplication - Based on primary keys, the data is De-duplicated and the latest records are only loaded into the consolidated stage tables
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- Standardization -
	- Currency Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local currency of the corresponding marketplace/store/account. Values that are in local currency are standardized by converting to desired currency using Daton Exchange Rates data.
	  Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this](https://github.com/saras-daton/currency_exchange_rates)
	- Time Zone Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local timezone of the corresponding marketplace/store/account. DateTime values that are in local timezone are standardized by converting to specified timezone using input offset hours.

#### Prerequisite 
Daton Integrations for  - {{Amazon Seller Partner}}, Exchange Rates(Optional, if currency conversion is not required)

# Installation & Configuration

## Installation Instructions

If you haven't already, you will need to create a packages.yml file in your DBT project. Include this in your `packages.yml` file

```yaml
packages:
  - package: saras-daton/amazon_sellerpartner
    version: {{1.0.0}}
```

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
    +schema: custom_schema_name
```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file under the above variables. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

```yaml
vars:
    currency_conversion_flag: False
    timezone_conversion_flag: False
    raw_table_timezone_offset_hours: {
    "edm-saras.EDM_Daton.Brand_US_AmazonSellerCentral_FlatFileAllOrdersReportbyLastUpdate":7,
    "edm-saras.EDM_Daton.Brand_US_AmazonSellerCentral_ListOrder":5,
    "edm-saras.EDM_Daton.Brand_US_AmazonSellerCentral_FBAAmazonFulfilledShipmentsReport":6,
    "edm-saras.EDM_Daton.Brand_US_AmazonSellerCentral_InventoryLedgerDetailedReport":8,
    "EDM.EDM.BRAND_US_AMAZONSELLERCENTRAL_SF_FBAAMAZONFULFILLEDSHIPMENTSREPORT":8,
    "EDM.EDM.BRAND_US_AMAZONSELLERCENTRAL_SF_LISTORDER":8,
    "EDM.EDM.BRAND_US_AMAZONSELLERCENTRAL_SF_FLATFILEALLORDERSREPORTBYLASTUPDATE":8,
    "EDM.EDM.BRAND_US_AMAZONSELLERCENTRAL_SF_INVENTORYLEDGERDETAILEDREPORT":8
    }
```

### Currency Conversion 

To enable currency conversion, which produces two columns - exchange_currency_rate & exchange_currency_code, please mark the currency_conversion_flag as True. By default, it is False.

### Timezone Conversion 

To enable timezone conversion, which converts the datetime columns from local timezone to given timezone, please mark the timezone_conversion_flag f as True in the dbt_project.yml file, by default, it is False
Additionally, you need to provide offset hours for each raw table

timezone_conversion_flag: False
raw_table_timezone_offset_hours: {
    "edm-saras.EDM_Daton.Brand_US_AmazonSellerCentral_FlatFileAllOrdersReportbyLastUpdate":7,
    "edm-saras.EDM_Daton.Brand_US_AmazonSellerCentral_ListOrder":5,
    "edm-saras.EDM_Daton.Brand_US_AmazonSellerCentral_FBAAmazonFulfilledShipmentsReport":6,
    "edm-saras.EDM_Daton.Brand_US_AmazonSellerCentral_InventoryLedgerDetailedReport":8,
    "EDM.EDM.BRAND_US_AMAZONSELLERCENTRAL_SF_FBAAMAZONFULFILLEDSHIPMENTSREPORT":8,
    "EDM.EDM.BRAND_US_AMAZONSELLERCENTRAL_SF_LISTORDER":8,
    "EDM.EDM.BRAND_US_AMAZONSELLERCENTRAL_SF_FLATFILEALLORDERSREPORTBYLASTUPDATE":8,
    "EDM.EDM.BRAND_US_AMAZONSELLERCENTRAL_SF_INVENTORYLEDGERDETAILEDREPORT":8
    }

### Table Partitions

To enable partitioning for the tables, please mark table_partition_flag variable as True. By default, it is False.

### Table Exclusions

Setting these table exclusions will remove the modelling enabled for the below models. By declaring the model names as variables as above and marking them as False, they get disabled. Refer the table below for model details. By default, all tables are created. ********************************

## Models

This package contains models from the Amazon Selling Partner API which includes reports on {{sales, margin, inventory, product}}. Please follow this to get more details about [models]{{(https://docs.google.com/spreadsheets/d/1OaJnVpBrPZaBusJXBHrT8dhnD2zctWMmSRN__WLQsl0/edit?usp=sharing)}}. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Customer | [ListOrder](models/Customer/ListOrder.sql)  | A list orders along with the customer details |
|Inventory | [FBAManageInventoryHealthReport](models/AmazonSellerCentral/FBAManageInventoryHealthReport.sql)  | A detailed report which gives details about inventory age , current inventory levels, recommended inventory levels |
|Inventory | [FBAManageInventory](models/AmazonSellerCentral/FBAManageInventory.sql)  | A list of ad groups associated with the accountA report which gives details about inventory movement - inbound, outbound, sellable |
|Inventory | [InventoryLedgerDetailedReport](models/AmazonSellerCentral/InventoryLedgerDetailedReport.sql)| A report about available quantity at the warehouse level |
|Financial Events | [OrderFees](models/AmazonSellerCentral/OrderFees.sql)| A list of fees associated with the shipment item. |
|Financial Events | [OrderPromotions](models/AmazonSellerCentral/OrderPromotions.sql)| A list of promotions which gives the amount of promotional discount applied to the item at an item & order level.|
|Financial Events | [OrderRevenue](models/AmazonSellerCentral/OrderRevenue.sql)| A list of shipment items which includes order & product level revenue |
|Financial Events | [OrderTaxes](models/AmazonSellerCentral/OrderTaxes.sql)| A list of order taxes |
|Financial Events | [RefundFees](models/AmazonSellerCentral/RefundFees.sql)| A list of fees associated with the refunded item.	 |
|Financial Events | [RefundPromotions](models/AmazonSellerCentral/RefundPromotions.sql)|A list of promotions which gives the amount of promotional discount applied to the item at an refunded item level. |
|Financial Events | [RefundRevenue](models/AmazonSellerCentral/RefundRevenue.sql)| A list of refunded items which includes refund & product level revenue |
|Financial Events | [RefundTaxes](models/AmazonSellerCentral/RefundTaxes.sql)| A list of refund taxes |
|Product | [CatalogItemsSummary](models/AmazonSellerCentral/CatalogItemsSummary.sql)| A list of product summary, manufacturer & dimensions |
|Product | [AllListingsReport](models/AmazonSellerCentral/AllListingsReport.sql)|  listing report with details about all types of listings |
|Returns | [FBAReturnsReport](models/AmazonSellerCentral/FBAReturnsReport.sql)|Returns report of the orders fulfilled by Amazon |
|Returns | [FlatFileReturnsReportByReturnDate](models/AmazonSellerCentral/FlatFileReturnsReportByReturnDate.sql)|Returns report of the orders fulfilled by Merchant |
|Sales | [FBAAmazonFulfilledShipmentsReport](models/AmazonSellerCentral/FBAAmazonFulfilledShipmentsReport.sql)|Orders report with shipment details included |
|Sales | [FlatFileAllOrdersReportByLastUpdate](models/AmazonSellerCentral/FlatFileAllOrdersReportByLastUpdate.sql)|Order & Item Level report |
|Sales | [SalesAndTrafficReportByChildASIN](models/AmazonSellerCentral/SalesAndTrafficReportByChildASIN.sql)|Provides sales & traffic at SKU level that we see in the Business Report in the UI |

## Resources:
- Have questions, feedback, or need [help](https://calendly.com/priyanka-vankadaru/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}
