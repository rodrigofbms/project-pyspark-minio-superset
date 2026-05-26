-- Creating each schema from MinIO layer in hive, for later acess all data in a table format
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


select * from delta.gold.gold_sales_per_employee 
order by employee_id 