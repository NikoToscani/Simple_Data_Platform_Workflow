/*
 * get table size
 */
select pg_size_pretty(pg_total_relation_size('schema_name_here.table_name_here')) as size;
/*
 * sales
 */
select pg_size_pretty(pg_total_relation_size('sales')) as size;
/*
 * plan
 */
select pg_size_pretty(pg_total_relation_size('plan')) as size;

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
 * list partitions of schema_name_here.table_name_here
 */
select
    partitiontablename,
    partitionrangestart,
    partitionrangeend,
    partitionboundary
from pg_partitions
where
	schemaname = 'schema_name_here'
	AND
    tablename = 'table_name_here'
order by partitionrangestart;
/*
 * row distribution in partitions of parent table
 */
SELECT 
    tableoid::regclass AS partition_name,
    COUNT(*) AS record_count
FROM 
    table_name_here
GROUP BY 
    tableoid
ORDER BY
    partition_name;
/*
 * SALES
 */
select table_name, ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'sales'
order by ordinal_position;

select count(*) from sales;

select * from sales limit 10;

select
    partitiontablename, partitionrangestart, partitionrangeend, partitionboundary
from pg_partitions
where schemaname = 'my_schema' AND tablename = 'sales'
order by partitionrangestart;

SELECT tableoid::regclass AS partition_name, COUNT(*) AS record_count
FROM sales
GROUP BY tableoid
ORDER BY partition_name;
/*
 * PLAN
 */
select table_name, ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'plan'
order by ordinal_position;

select count(*) from plan;

select * from plan limit 10;

select
    partitiontablename, partitionrangestart, partitionrangeend, partitionboundary
from pg_partitions
where schemaname = 'my_schema' AND tablename = 'plan'
order by partitionrangestart;

SELECT tableoid::regclass AS partition_name, COUNT(*) AS record_count
FROM plan
GROUP BY tableoid
ORDER BY partition_name;
/*
 * PRICE
 */
select table_name, ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'price'
order by ordinal_position;

select count(*) from price;

select * from price limit 10;
/*
 * PRODUCT
 */
select table_name, ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'product'
order by ordinal_position;

select count(*) from product;

select * from product limit 10;
/*
 * CHANEL
 */
select table_name, ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'chanel'
order by ordinal_position;

select count(*) from chanel;

select * from chanel limit 10;
/*
 * REGION
 */
select table_name, ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'region'
order by ordinal_position;

select count(*) from region;

select * from region limit 10;
/*
 * MART
 */
select table_name, ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'plan_fact_202101'
order by ordinal_position;

select count(*) from my_schema.plan_fact_202101;

select * from my_schema.plan_fact_202101 limit 10;

select * from my_schema.v_plan_fact limit 10;
/*
 * LOGS
 */
SELECT * 
FROM my_schema.logs 
order by log_id desc
limit 20;






