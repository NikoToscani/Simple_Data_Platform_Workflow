/*
 * list table data structure information
 */
select
	table_name,
	ordinal_position,
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
from
    INFORMATION_SCHEMA.COLUMNS
where
    table_name = 'table_name_here'
order by
    ordinal_position;
/*
 * row quantity
 */
select count(*) from table_name_here;
/*
 * data sample
 */
select * from table_name_here limit 10;
/*
 * EXT_SALES
 */
select table_name,	ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'ext_sales'
order by ordinal_position;

select count(*) from ext_sales;

select * from ext_sales limit 10;
/*
 * EXT_PLAN
 */
select table_name,	ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'ext_plan'
order by ordinal_position;

select count(*) from ext_plan;

select * from ext_plan limit 10;
/*
 * EXT_PRICE
 */
select table_name,	ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'ext_price'
order by ordinal_position;

select count(*) from ext_price;

select * from ext_price limit 10;
/*
 * EXT_PRODUCT
 */
select table_name,	ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'ext_product'
order by ordinal_position;

select count(*) from ext_product;

select * from ext_product limit 10;
/*
 * EXT_CHANEL
 */
select table_name,	ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'ext_chanel'
order by ordinal_position;

select count(*) from ext_chanel;

select * from ext_chanel limit 10;
/*
 * EXT_REGION
 */
select table_name,	ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'ext_region'
order by ordinal_position;

select count(*) from ext_region;

select * from ext_region limit 10;






