# Amazon Seller Partner Data Modelling
This DBT package models the Amazon Selling Partner data coming from [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

This package would be performing the following funtions:

- Consolidation - Different marketplaces & different brands would have similar tables. Helps in consolidating all the tables into one final stage table 
- Deduplication - Based on primary keys , the tables are deduplicated and the latest records are only loaded into the stage models
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- (Optional) Currency Conversion - Based on the currency input, a couple of currency columns are generated to aid in the currency conversion - (Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this]())
- (Optional) Time Zone Conversion - Based on the time zone input, the relevant datetime columns are replaced with the converted values

#### Prerequisite 
Daton Connectors for Amazon Seller Partner Data - Amazon Seller Partner, Exchange Rates(Optional)

# Installation & Configuration

## Installation Instructions

If you haven't already, you will need to create a packages.yml file in your project. Include this in your `packages.yml` file

```yaml
packages:
  - package: daton/amazon_sellerpartner_bigquery
    version: 0.1.0
```

# Configuration 

## Required Variables

This package assumes that you have an existing DBT project with a BigQuery profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.

```yaml
vars:
    raw_projectid: "your_gcp_project"
    raw_dataset: "your_amazon_sellerpartner_dataset"
```

## Schema Change

We will create the models under the schema (<target_schema>_stg_amazon). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yml
models:
  amazon_sellerpartner_bigquery:
    +schema: custom_schema_name # leave blank for just the target_schema
```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file under the above variables. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

```yaml
vars:
    currency_conversion_flag: False
    timezone_conversion_flag:
        amazon_sellerpartner: False
    timezone_conversion_hours: 7
    table_partition_flag: False
    SponsoredBrands_Portfolio: True
    brand_consolidation_flag: False
    brand_name_position: 0
    brand_name: "Amazon Seller Name"
```

### Currency Conversion 

To enable currency conversion, which produces two columns - conversion_rate, conversion_currency based on the data from the Exchange Rates Connector from Daton.  please mark the currency conversion flag as True. By default, it is False.

### Timezone Conversion 

To enable timezone conversion, which converts the major date columns according to given timezone, please mark the time zone conversion variable as True in the dbt_project.yml file. The data is available at UTC timezone and by setting the hr variable, it will be offset by the specified number of hours.(Eg: 7,8,-7,-11 etc) By default, it is False.

### Table Partitions

To enable partitioning for the tables, please mark table_partition_flag variable as True. By default, it is False.

### Table Exclusions

Setting these table exclusions will remove the modelling enabled for the below models. By declaring the model names as variables as above and marking them as False, they get disabled. Refer the table below for model details. By default, all tables are created. 

### Brand Consolidation

The Amazon Seller Name would be called the Brand name in this case. If you sell more than one brand, then brand consolidation can be enabled. Brand name positions (Eg: 0/1/2) gets the brand name from the integration name based on the location you have given. In case there is only a single brand, adding the name in brand name variable adds the column to the tables. By default, brand consolidation flag is False and brand name variable needs to be set.

## Scheduling the Package for refresh

The ad tables that are being generated as part of this package are enabled for incremental refresh and can be scheduled by creating the job in Production Environment by giving the below command.

```
dbt run --select amazon_sellerpartner_bigquery
```

## Models

This package contains models from the Amazon Selling Partner API which includes reports on sales, margin, inventory, product. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Customer | [ListOrder](models/Customer/ListOrder.sql)  | A list orders along with the customer details |
|Inventory | [FBAManageInventoryHealthReport](models/Inventory/FBAManageInventoryHealthReport.sql)  | A detailed report which gives details about inventory age , current inventory levels, recommended inventory levels |
|Inventory | [FBAManageInventory](models/Inventory/FBAManageInventory.sql)  | A list of ad groups associated with the accountA report which gives details about inventory movement - inbound, outbound, sellable |
|Inventory | [InventoryLedgerDetailedReport](models/Inventory/InventoryLedgerDetailedReport.sql)| A report about available quantity at the warehouse level |
|Financial Events | [FE_OrderFees](models/Margin/FE_OrderFees.sql)| A list of fees associated with the shipment item. |
|Financial Events | [FE_OrderPromotions](models/Margin/FE_OrderPromotions.sql)| A list of promotions which gives the amount of promotional discount applied to the item at an item & order level.|
|Financial Events | [FE_OrderRevenue](models/Margin/FE_OrderRevenue.sql)| A list of shipment items which includes order & product level revenue |
|Financial Events | [FE_OrderTaxes](models/Margin/FE_OrderTaxes.sql)| A list of order taxes |
|Financial Events | [FE_RefundFees](models/Margin/FE_RefundFees.sql)| A list of fees associated with the refunded item.	 |
|Financial Events | [FE_RefundPromotions](models/Margin/FE_RefundPromotions.sql)|A list of promotions which gives the amount of promotional discount applied to the item at an refunded item level. |
|Financial Events | [FE_RefundRevenue](models/Margin/FE_RefundRevenue.sql)| A list of refunded items which includes refund & product level revenue |
|Financial Events | [FE_RefundTaxes](models/Margin/FE_RefundTaxes.sql)| A list of refund taxes |
|Product | [CatalogItemsSummary](models/Product/CatalogItemsSummary.sql)| A list of product summary, manufacturer & dimensions |
|Product | [AllListingsReport](models/Product/AllListingsReport.sql)|  listing report with details about all types of listings |
|Returns | [FBAReturnsReport](models/Returns/FBAReturnsReport.sql)|Returns report of the orders fulfilled by Amazon |
|Returns | [FlatFileReturnsReportByReturnDate](models/Returns/FlatFileReturnsReportByReturnDate.sql)|Returns report of the orders fulfilled by Merchant |
|Sales | [FBAAmazonFulfilledShipmentsReport](models/Sales/FBAAmazonFulfilledShipmentsReport.sql)|Orders report with shipment details included |
|Sales | [FlatFileAllOrdersReportByLastUpdate](models/Sales/FlatFileAllOrdersReportByLastUpdate.sql)|Order & Item Level report |
|Sales | [SalesAndTrafficReportByChildASIN](models/Traffic/SalesAndTrafficReportByChildASIN.sql)|Provides sales & traffic at SKU level that we see in the Business Report in the UI |

## Resources:
- Have questions, feedback, or need [help](https://calendly.com/priyanka-vankadaru/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a DBT account & connect to Bigquery
