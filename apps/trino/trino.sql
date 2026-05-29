-- Creating each schema from MinIO in hive, for later acess all data in a table format
create schema hive.bronze with (location='s3a://bronze/')

create schema hive.silver with (location='s3a://silver/')

create schema hive.gold with (location='s3a://gold/')


-- Gold tables
call delta.system.register_table(
	schema_name => 'gold',
	table_name => 'gold_quantity_sales_per_product',
	table_location => 's3a://gold/adventureworks/gold_quantity_sales_per_product'
);

call delta.system.register_table(
	schema_name => 'gold',
	table_name => 'gold_quantity_sales_per_ship_method',
	table_location => 's3a://gold/adventureworks/gold_quantity_sales_per_ship_method'
);

call delta.system.register_table(
	schema_name => 'gold',
	table_name => 'gold_sales_by_country',
	table_location => 's3a://gold/adventureworks/gold_sales_by_country'
);

call delta.system.register_table(
	schema_name => 'gold',
	table_name => 'gold_sales_per_card_type',
	table_location => 's3a://gold/adventureworks/gold_sales_per_card_type'
);

call delta.system.register_table(
	schema_name => 'gold',
	table_name => 'gold_sales_per_city_country',
	table_location => 's3a://gold/adventureworks/gold_sales_per_city_country'
);

call delta.system.register_table(
	schema_name => 'gold',
	table_name => 'gold_sales_per_customer',
	table_location => 's3a://gold/adventureworks/gold_sales_per_customer'
);

call delta.system.register_table(
	schema_name => 'gold',
	table_name => 'gold_sales_per_employee',
	table_location => 's3a://gold/adventureworks/gold_sales_per_employee'
);


-- Silver tables
call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_countryregioncurrency',
	table_location => 's3a://silver/adventureworks/silver_sales_countryregioncurrency'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_creditcard',
	table_location => 's3a://silver/adventureworks/silver_sales_creditcard'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_currency',
	table_location => 's3a://silver/adventureworks/silver_sales_currency'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_currencyrate',
	table_location => 's3a://silver/adventureworks/silver_sales_currencyrate'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_customer',
	table_location => 's3a://silver/adventureworks/silver_sales_customer'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_personcreditcard',
	table_location => 's3a://silver/adventureworks/silver_sales_personcreditcard'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_salesorderdetail',
	table_location => 's3a://silver/adventureworks/silver_sales_salesorderdetail'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_salesorderheader',
	table_location => 's3a://silver/adventureworks/silver_sales_salesorderheader'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_salesorderheadersalesreason',
	table_location => 's3a://silver/adventureworks/silver_sales_salesorderheadersalesreason'
);


call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_salesperson',
	table_location => 's3a://silver/adventureworks/silver_sales_salesperson'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_salespersonquotahistory',
	table_location => 's3a://silver/adventureworks/silver_sales_salespersonquotahistory'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_salesreason',
	table_location => 's3a://silver/adventureworks/silver_sales_salesreason'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_salestaxrate',
	table_location => 's3a://silver/adventureworks/silver_sales_salestaxrate'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_salesterritory',
	table_location => 's3a://silver/adventureworks/silver_sales_salesterritory'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_salesterritoryhistory',
	table_location => 's3a://silver/adventureworks/silver_sales_salesterritoryhistory'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_shoppingcartitem',
	table_location => 's3a://silver/adventureworks/silver_sales_shoppingcartitem'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_specialoffer',
	table_location => 's3a://silver/adventureworks/silver_sales_specialoffer'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_specialofferproduct',
	table_location => 's3a://silver/adventureworks/silver_sales_specialofferproduct'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_sales_store',
	table_location => 's3a://silver/adventureworks/silver_sales_store'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_humanresources_employee',
	table_location => 's3a://silver/adventureworks/silver_humanresources_employee'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_humanresources_department',
	table_location => 's3a://silver/adventureworks/silver_humanresources_department'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_person_countryregion',
	table_location => 's3a://silver/adventureworks/silver_person_countryregion'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_person_person',
	table_location => 's3a://silver/adventureworks/silver_person_person'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_person_stateprovince',
	table_location => 's3a://silver/adventureworks/silver_person_stateprovince'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_person_address',
	table_location => 's3a://silver/adventureworks/silver_person_address'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_purchasing_shipmethod',
	table_location => 's3a://silver/adventureworks/silver_purchasing_shipmethod'
);

call delta.system.register_table(
	schema_name => 'silver',
	table_name => 'silver_production_product',
	table_location => 's3a://silver/adventureworks/silver_production_product'
);