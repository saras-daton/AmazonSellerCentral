# Amazon Seller Partner Data Modelling
This DBT package models the Amazon Selling Partner data coming from [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

Daton Connectors for Amazon Selling Partner Data - Amazon Selling Partner

This package would be performing the following funtions:

- Consolidation - Different marketplaces & different brands would have similar tables. Helps consolidated all the tables into one final stage table 
- Deduplication - Based on primary keys , the tables are deduplicated and the latest records are only loaded into the stage table
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- (Optional) Currency Conversion - Based on the currency input, a couple of currency columns are generated to aid in the currency conversion - (Prequiste - Exchange Rates connector in Daton needs to be present - Refer [this]())
- (Optional) Time Zone Conversion - Based on the time zone input, a timezone column with the converted timestamp is created

# Installation & Configuration

## Installation Instructions

If you haven't already, you will need to create a packages.yml file in your project. Include this in your `packages.yml` file

```yaml
packages:
  - package: daton/amazon_sellerpartner_bigquery
    version: [">=0.1.0", "<0.3.0"]
```

# Configuration 

## Required Variables

This package assumes that you have an existing DBT project with a BigQuery profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.

```
vars:
    raw_projectid: "your_gcp_project"
    raw_dataset: "your_amazon_sellerpartner_dataset"
```

## Optional Variables

### Currency Conversion 

To enable currency conversion, which produces two columns - conversion_rate, conversion_currency based on the data from the Exchange Rates Connector from Daton.  please add the following in the dbt_project.yml file. By default, it is False.

```
vars:
    currency_conversion_flag: False
```

### Timezone Conversion 

To enable timezone conversion, which converts the major date columns according to given timezone,.  please add the following in the dbt_project.yml file. The data is available at UTC timezone and by setting the hr variable, it will be offset by the specified number of hours.(Eg: 7,8,-7,-11 etc) By default, it is False.

```
vars:
    timezone_conversion_flag: False
    timezone_conversion_hours: 7
```

### Table Partitions

To enable partitioning for the tables, please add the following in the dbt_project.yml file. By default, it is False.

```
vars:
    table_partition_flag: False
```

### Table Exclusions

Setting these table exclusions will remove the modelling enabled for the below tables. By declaring the model names as variables as below, they get disabled. Refer the table below for model details. By default, these tables are tagged True. 

```
vars:
    ListOrder: True
```

## Scheduling the Package for refresh

The ad tables that are being generated as part of this package are enabled for incremental refresh and can be scheduled by creating the job in Production Environment by giving the below command.

```
dbt run --select amazon_sellingpartner_bigquery
```

## Models

This package contains models from the Amazon Selling Partner API which includes reports on sales, margin, inventory, product. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Customer | [ListOrder](models/Customer/ListOrder.sql)  | A list orders along with the customer details |
|Inventory | [FBAManageInventoryHealthReport](models/Inventory/FBAManageInventoryHealthReport.sql)  | A detailed report which gives details about inventory age , current inventory levels, recommended inventory levels |
|Inventory | [FBAManageInventory](models/Inventory/FBAManageInventory.sql)  | A list of ad groups associated with the accountA report which gives details about inventory movement - inbound, outbound, sellable |
|Inventory | [InventoryLedgerDetailedReport](models/Inventory/InventoryLedgerDetailedReport.sql)| A report about available quantity at the warehouse level |
|Financials | [ListFinancialEvents_OrderFees](models/Margin/ListFinancialEvents_OrderFees.sql)| A list of fees associated with the shipment item. |
|Financials | [ListFinancialEvents_OrderPromotions](models/Margin/ListFinancialEvents_OrderPromotions.sql)| A list of promotions which gives the amount of promotional discount applied to the item at an item & order level.|
|Financials | [ListFinancialEvents_OrderRevenue](models/Margin/ListFinancialEvents_OrderRevenue.sql)| A list of shipment items which includes order & product level revenue |
|Financials | [ListFinancialEvents_OrderTaxes](models/Margin/ListFinancialEvents_OrderTaxes.sql)| A list of order taxes |
|Financials | [ListFinancialEvents_RefundFees](models/Margin/ListFinancialEvents_RefundFees.sql)| A list of fees associated with the refunded item.	 |
|Financials | [ListFinancialEvents_RefundPromotions](models/Margin/ListFinancialEvents_RefundPromotions.sql)|A list of promotions which gives the amount of promotional discount applied to the item at an refunded item level. |
|Financials | [ListFinancialEvents_RefundRevenue](models/Margin/ListFinancialEvents_RefundRevenue.sql)| A list of refunded items which includes refund & product level revenue |
|Financials | [ListFinancialEvents_RefundTaxes](models/Margin/ListFinancialEvents_RefundTaxes.sql)| A list of refund taxes |
|Product | [CatalogItemsSummary](models/Product/CatalogItemsSummary.sql)| A list of product summary, manufacturer & dimensions |
|Product | [AllListingsReport](models/Product/AllListingsReport.sql)|  listing report with details about all types of listings |
|Returns | [FBAReturnsReport](models/Returns/FBAReturnsReport.sql)|Returns report of the orders fulfilled by Amazon |
|Returns | [FlatFileReturnsReportByReturnDate](models/Returns/FlatFileReturnsReportByReturnDate.sql)|Returns report of the orders fulfilled by Merchant |
|Sales | [FBAAmazonFulfilledShipmentsReport](models/Sales/FBAAmazonFulfilledShipmentsReport.sql)|Orders report with shipment details included |
|Sales | [FlatFileAllOrdersReportByLastUpdate](models/Sales/FlatFileAllOrdersReportByLastUpdate.sql)|Order & Item Level report |
|Sales | [SalesAndTrafficReportByChildASIN](models/Traffic/SalesAndTrafficReportByChildASIN.sql)|Provides sales & traffic at SKU level that we see in the Business Report in the UI |

## Resources:
- Have questions, feedback, or need [help](https://meetings.hubspot.com/balaji-kolli/)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a DBT account & connect to Bigquery
