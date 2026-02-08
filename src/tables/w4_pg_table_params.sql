/*
 * sales
 */
select count(*) from gp.sales limit 10;
select * from gp.sales limit 10;
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
    table_name = 'sales'
order by
    ordinal_position;

/*
 * plan
 */
select count(*) from gp.plan limit 10;
select * from gp.plan limit 10;
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
    table_name = 'plan'
order by
    ordinal_position;







