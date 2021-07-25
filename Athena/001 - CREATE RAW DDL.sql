CREATE EXTERNAL TABLE IF NOT EXISTS SALES_DATABASE.SALES_RAW (
  `ordernumber` int,
  `quantityordered` int,
  `priceeach` float,
  `orderlinenumber` int,
  `sales` float,
  `orderdate` string,
  `status` string,
  `qtr_id` int,
  `month_id` int,
  `year_id` int,
  `productline` string,
  `msrp` int,
  `productcode` string,
  `customername` string,
  `phone` string,
  `addressline1` string,
  `addressline2` string,
  `city` string,
  `state` string,
  `postalcode` string,
  `country` string,
  `territory` string,
  `contactlastname` string,
  `contactfirstname` string,
  `dealsize` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = '1',
  'skip.header.line.count' = '1'
) LOCATION 's3://lucas-lakehouse-demo/raw/'
TBLPROPERTIES ('has_encrypted_data'='false');



SELECT * FROM "sales_database"."fact_sales"  FS
LEFT JOIN "sales_database"."dim_locale" DL
ON FS.locale_id = DL.locale_id
LEFT JOIN "sales_database"."dim_client" DC
ON FS.client_id = DC.client_id
limit 10;

