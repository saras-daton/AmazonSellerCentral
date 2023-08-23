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
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_InventoryLedgerDetailedReport":-7,
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_CatalogItems":-7,
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_AllListingsReport":-7,
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_FBAManageInventory":-7,
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_FBAManageInventoryHealthReport":-7,
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_FBAReturnsReport":-7,
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_FlatFileReturnsReportbyReturnDate":-7,
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_ListFinancialEvents":-7,
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_ListingOffersForASIN":-7,
    "Amazon.SellerCentral.Brand_US_AmazonSellerCentral_SalesAndTrafficReportByChildASIN":-7
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
| ------------------------- | ---------------| ----------------------- |
|Customer | [ListOrder](models/Customer/ListOrder.sql)  | A list orders along with the customer details | PurchaseDate,amazonorderid,marketplaceName,sellingPartnerId | PurchaseDate | LastUpdateDate,PurchaseDate,amazonorderid |
|Inventory | [FBAManageInventoryHealthReport](models/AmazonSellerCentral/FBAManageInventoryHealthReport.sql)  | A detailed report which gives details about inventory age , current inventory levels, recommended inventory levels | snapshot_date,asin,sku,marketplaceId,sellingPartnerId | snapshot_date | snapshot_date,asin,sku |
|Inventory | [FBAManageInventory](models/AmazonSellerCentral/FBAManageInventory.sql)  | A list of ad groups associated with the accountA report which gives details about inventory movement - inbound, outbound, sellable | ReportstartDate,sku,marketplaceId | ReportstartDate | ReportstartDate,sku |
|Inventory | [InventoryLedgerDetailedReport](models/AmazonSellerCentral/InventoryLedgerDetailedReport.sql)| A report about available quantity at the warehouse level | date,asin,fulfillment_center,msku,event_type,reference_id,quantity,disposition,marketplaceid,sellingPartnerId | date | date,asin,msku |
|Financial Events | [ListFinancialEventsOrderFees](models/AmazonSellerCentral/ListFinancialEventsOrderFees.sql)| A list of fees associated with the shipment item. | ShipmentEventlist_PostedDate,ShipmentEventlist_MarketplaceName,ShipmentEventlist_AmazonOrderId,ItemFeeList_FeeType,_seq_id | ShipmentEventlist_PostedDate | ShipmentEventlist_PostedDate,ShipmentEventlist_MarketplaceName,ShipmentEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsOrderPromotions](models/AmazonSellerCentral/ListFinancialEventsOrderPromotions.sql)| A list of promotions which gives the amount of promotional discount applied to the item at an item & order level.| ShipmentEventlist_PostedDate,ShipmentEventlist_MarketplaceName,ShipmentEventlist_AmazonOrderId,PromotionList_PromotionType,_seq_id | ShipmentEventlist_PostedDate | ShipmentEventlist_PostedDate,ShipmentEventlist_MarketplaceName,ShipmentEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsOrderRevenue](models/AmazonSellerCentral/ListFinancialEventsOrderRevenue.sql)| A list of shipment items which includes order & product level revenue | ShipmentEventlist_PostedDate,ShipmentEventlist_MarketplaceName,ShipmentEventlist_AmazonOrderId,ItemChargeList_ChargeType,_seq_id | ShipmentEventlist_PostedDate | ShipmentEventlist_PostedDate,ShipmentEventlist_MarketplaceName,ShipmentEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsOrderTaxes](models/AmazonSellerCentral/ListFinancialEventsOrderTaxes.sql)| A list of order taxes | ShipmentEventlist_PostedDate,ShipmentEventlist_MarketplaceName,ShipmentEventlist_AmazonOrderId,TaxesWithheld_ChargeType,_seq_id | ShipmentEventlist_PostedDate | ShipmentEventlist_PostedDate,ShipmentEventlist_MarketplaceName,ShipmentEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsRefundFees](models/AmazonSellerCentral/ListFinancialEventsRefundFees.sql)| A list of fees associated with the refunded item.	 | RefundEventlist_PostedDate,RefundEventlist_MarketplaceName,RefundEventlist_AmazonOrderId,ItemFeeAdjustmentList_FeeType,_seq_id | RefundEventlist_PostedDate | RefundEventlist_PostedDate,RefundEventlist_MarketplaceName,RefundEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsRefundPromotions](models/AmazonSellerCentral/ListFinancialEventsRefundPromotions.sql)|A list of promotions which gives the amount of promotional discount applied to the item at an refunded item level. | RefundEventlist_PostedDate,RefundEventlist_MarketplaceName,RefundEventlist_AmazonOrderId,PromotionList_PromotionType,_seq_id | RefundEventlist_PostedDate | RefundEventlist_PostedDate,RefundEventlist_MarketplaceName,RefundEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsRefundRevenue](models/AmazonSellerCentral/ListFinancialEventsRefundRevenue.sql)| A list of refunded items which includes refund & product level revenue | RefundEventlist_PostedDate,RefundEventlist_MarketplaceName,RefundEventlist_AmazonOrderId,ItemChargeAdjustmentList_ChargeType,_seq_id | RefundEventlist_PostedDate | RefundEventlist_PostedDate,RefundEventlist_MarketplaceName,RefundEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsRefundTaxes](models/AmazonSellerCentral/ListFinancialEventsRefundTaxes.sql)| A list of refund taxes | RefundEventlist_PostedDate,RefundEventlist_MarketplaceName,RefundEventlist_AmazonOrderId,TaxesWithheld_ChargeType,_seq_id | RefundEventlist_PostedDate | RefundEventlist_PostedDate,RefundEventlist_MarketplaceName,RefundEventlist_AmazonOrderId |
|Financial Events | [ListFinancialEventsServicefees](models/AmazonSellerCentral/ListFinancialEventsServicefees.sql)| A list of service level fees | RequestStartDate,marketplaceId,ServiceFeeEventList_FeeReason,FeeList_FeeType,ServiceFeeEventList_SellerSKU,ServiceFeeEventList_FeeDescription,_seq_id | RequestStartDate | RequestStartDate,marketplaceName,sellingPartnerId |
|Product | [CatalogItems](models/AmazonSellerCentral/CatalogItems.sql)| A list of products, manufacturer & dimensions | summaries_brandName,ReferenceASIN,summaries_modelNumber,marketplaceId,sellingPartnerId | RequestStartDate | RequestStartDate,summaries_brandName,ReferenceASIN |
|Product | [AllListingsReport](models/AmazonSellerCentral/AllListingsReport.sql)|  listing report with details about all types of listings | seller_sku,listing_id | - | ReportstartDate,seller_sku |
|Returns | [FBAReturnsReport](models/AmazonSellerCentral/FBAReturnsReport.sql)|Returns report of the orders fulfilled by Amazon | return_date,asin,sku,order_id,fnsku,license_plate_number,fulfillment_center_id,_seq_id,marketplaceId | return_date | ReportstartDate,return_date,asin,sku |
|Returns | [FlatFileReturnsReportByReturnDate](models/AmazonSellerCentral/FlatFileReturnsReportByReturnDate.sql)|Returns report of the orders fulfilled by Merchant | Return_request_date,Order_ID,ASIN,marketplaceId | Return_request_date | ReportstartDate,Return_request_date,ASIN,Merchant_SKU,Order_ID |
|Sales | [FBAAmazonFulfilledShipmentsReport](models/AmazonSellerCentral/FBAAmazonFulfilledShipmentsReport.sql)|Orders report with shipment details included | purchase_date,sku,amazon_order_id,marketplaceName,_seq_id | purchase_date | reporting_date,purchase_date,sku,amazon_order_id |
|Sales | [FlatFileAllOrdersReportByLastUpdate](models/AmazonSellerCentral/FlatFileAllOrdersReportByLastUpdate.sql)|Order & Item Level report | purchase_date,amazon_order_id,asin,sku,_seq_id | purchase_date | last_updated_date,purchase_date,asin,sku,amazon_order_id |
|Sales | [SalesAndTrafficReportByChildASIN](models/AmazonSellerCentral/SalesAndTrafficReportByChildASIN.sql)|Provides sales & traffic at SKU level that we see in the Business Report in the UI | date,parentAsin,childAsin,marketplaceId,sellingPartnerId | date | ReportstartDate,date,parentAsin,childAsin |
|Product | [ListingOffersForASIN](models/AmazonSellerCentral/ListingOffersForASIN.sql)|  listing report with details about all types of listings offers at ASIN level | ASIN,itemCondition,sellingPartnerId | RequeststartDate | RequeststartDate,ASIN,itemCondition |

## DBT Tests

The tests property defines assertions about a column, table, or view. The property contains a list of generic tests, referenced by name, which can include the four built-in generic tests available in dbt. For example, you can add tests that ensure a column contains no duplicates and zero null values. Any arguments or configurations passed to those tests should be nested below the test name.

| **Tests**  | **Description** |
| :--  | ------------------------------------------- |
| [Not Null Test](https://docs.getdbt.com/reference/resource-properties/tests#testing-an-expression)  | This test validates that there are no null values present in a column |
| [Data Recency Test](https://github.com/dbt-labs/dbt-utils/blob/main/macros/generic_tests/recency.sql)  | This is used to check for issues with data refresh within {{ x }} days, please specify the value of number of days at {{ x }} |
| [Accepted Value Test](https://docs.getdbt.com/reference/resource-properties/tests#accepted_values)  | This test validates that all of the values in a column are present in a supplied list of values. If any values other than those provided in the list are present, then the test will fail, by default it consists of default values and this needs to be changed based on the project |
| [Uniqueness Test](https://docs.getdbt.com/reference/resource-properties/tests#testing-an-expression)  | This test validates that there are no duplicate values present in a field |

### Table Name: AllListingsReport

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `  seller_sku   `|        Yes        |                       |                         |         Yes         |
| `  listing_id   `|        Yes        |                       |                         |         Yes         |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: CatalogItems

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: FBAAmazonFulfilledShipmentsReport

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: FBAManageInventory

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: FBAManageInventoryHealthReport

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: FBAReturnsReport

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: FlatFileAllOrdersReportByLastUpdate

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: FlatFileReturnsReportByReturnDate

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: InventoryLedgerDetailedReport

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: ListFinancialEventsOrderFees

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: ListFinancialEventsOrderPromotions

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: ListFinancialEventsOrderRevenue

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: ListFinancialEventsOrderTaxes

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: ListFinancialEventsRefundFees

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: ListFinancialEventsRefundPromotions

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: ListFinancialEventsRefundRevenue

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: ListFinancialEventsRefundTaxes

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: ListFinancialEventsServiceFees

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| `      id       `|        Yes        |                       |                         |         Yes         |
| `    email      `|        Yes        |                       |                         |                     |
| `  created_at   `|        Yes        |                       |                         |                     |
| `  updated_at   `|        Yes        |      Yes (1 day)      |                         |                     |

### Table Name: ListOrder

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| ` PurchaseDate  `|        Yes        |      Yes (1 day)      |                         |         Yes         |
| ` amazonorderid `|        Yes        |                       |                         |         Yes         |
| `marketplaceName`|        Yes        |                       |                         |         Yes         |

### Table Name: ListingOffersForASIN

|   **Columns**    | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
|       :--        |        :-:        |          :-:          |           :-:           |         :-:         |
| `    brand      `|        Yes        |          Yes          |           Yes           |                     |
| `    store      `|        Yes        |                       |                         |                     |
| ` PurchaseDate  `|        Yes        |      Yes (1 day)      |                         |         Yes         |
| ` amazonorderid `|        Yes        |                       |                         |         Yes         |
| `marketplaceName`|        Yes        |                       |                         |         Yes         |


## Resources:
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}
