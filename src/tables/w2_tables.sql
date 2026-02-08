/*
 * AO-row таблицы подходят для переливки данных из источника в витрины
 * (запрос более 80% столбцов).
 * 
 * AO-column подходит, когда данные в таблице фильтруются или агрегируются,
 * а выборка происходит по ограниченному количеству столбцов (50% и меньше).
 */

/*
 * fact tables (all AO)
 */

/*
 * sales
 */
drop table my_schema.sales;

create table my_schema.sales(
	check_nm INT8, --PrimKey
	check_pos INT, --PrimKey
	material INT,
	region VARCHAR,
	dist_chan INT,
	quantity INT,
	"date" DATE
)
with (
    appendonly=true,
	orientation=row, --? depends on selects and other usage
	compresstype=zstd,
    compresslevel=1
)
--option 1
distributed by (check_nm)
    -- --option 2
    -- distributed randomly
partition by range ("date") (
--	start (date '2020-01-01') inclusive end (date '2025-01-01') exclusive every (interval '1 year'),
--	start (date '2025-01-01') inclusive end (date '2026-01-01') exclusive every (interval '3 months'),
    start (date '2020-01-01') inclusive end (date '2021-01-01') exclusive every (interval '1 year'),
    default partition other_sales
);

    -- --split default partition later on by:
    -- alter table sales
    -- split default partition start (date 'new_start') inclusive end (date 'new_end') exclusive
    -- --where 'new_start' ('2026-01-01') and 'new_end' ('2026-02-01') - beginning and end of new created partition

/*
 * plan
 */
drop table my_schema.plan;

create table my_schema.plan(
	"date" DATE, --PrimKey
	region VARCHAR, --PrimKey
	matdirec INT, --PrimKey
	dist_chan INT,  --PrimKey
    quantity INT
)
with (
    appendonly=true,
	orientation=row, --? depends on selects and other usage
	compresstype=zstd,
    compresslevel=1
)
--option 1.1
distributed by (matdirec)
    -- --option 1.2
    -- distributed by ("date")
    -- --option 2
    -- distributed replicated
partition by range ("date") (
--	start (date '2020-01-01') inclusive end (date '2025-01-01') exclusive every (interval '1 year'),
--	start (date '2025-01-01') inclusive end (date '2026-01-01') exclusive every (interval '3 months'),
    start (date '2020-01-01') inclusive end (date '2021-01-01') exclusive every (interval '1 year'),
    default partition other_plans
);

    -- --split default partition later on by:
    -- alter table plan
    -- split default partition start (date 'new_start') inclusive end (date 'new_end') exclusive
    -- --where 'new_start' ('2026-01-01') and 'new_end' ('2026-02-01') - beginning and end of new created partition

/*
 * dimension tables (all heap)
 */

/*
 * price
 */
drop table my_schema.price CASCADE;

create table my_schema.price(
	material INT, --PrimKey
	region VARCHAR, --PrimKey
	distr_chan INT, --PrimKey
	price INT,
    PRIMARY KEY (material, region, distr_chan)
)
with (
    appendonly=false
)
distributed replicated;

/*
 * product
 */
drop table my_schema.product CASCADE;

create table my_schema.product(
	material INT PRIMARY KEY, --PrimKey
	asgrp VARCHAR,
	brand INT,
	matcateg VARCHAR,
	matdirec INT,
	txt TEXT
)
with (
    appendonly=false
)
distributed replicated;

/*
 * chanel
 */
drop table my_schema.chanel CASCADE;

create table my_schema.chanel(
	distr_chan INT PRIMARY KEY, --PrimKey
	txtsh TEXT
)
with (
    appendonly=false
)
distributed replicated;

/*
 * region
 */
drop table my_schema.region CASCADE;

create table my_schema.region(
	region VARCHAR PRIMARY KEY, --PrimKey
	txt TEXT
)
with (
    appendonly=false
)
distributed replicated;






