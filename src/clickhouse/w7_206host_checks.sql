/*
 * инфо по ch_plan_fact
 */
select table_name, ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_schema = 'my_schema' AND table_name = 'ch_plan_fact'
order by ordinal_position;

select count(*) from my_schema.ch_plan_fact;

select * from my_schema.ch_plan_fact limit 10;

/*
 * инфо по ch_plan_fact_distr
 */
select table_name, ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_schema = 'my_schema' AND table_name = 'ch_plan_fact_distr'
order by ordinal_position;

select count(*) from my_schema.ch_plan_fact_distr;

select * from my_schema.ch_plan_fact_distr limit 10;

/*
 * show which replica and shard are on current host
 */
select * from system.macros;

/*
 * list available dictionaries at 'my_schema' db,
 * load and check them
 */
select database, name, status, last_successful_update_time, last_exception
from system.dictionaries
where database = 'my_schema';

select dictGet('my_schema.ch_price_dict', 'price', tuple(128047, 'R001', 1));
select dictGet('my_schema.ch_chanel_dict', 'txtsh', 2);
select dictGet('my_schema.ch_product_dict', 'txt', 1428824);
select dictGet('my_schema.ch_region_dict', 'txt', 'R001');



