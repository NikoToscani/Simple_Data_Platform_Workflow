/*
 * 1. Создайте базу данных std<номер пользователя> на 206 хосте.
 * (bd creation on all cluster hosts at once)
 */
DROP DATABASE my_schema on cluster default_cluster;

CREATE DATABASE my_schema ON CLUSTER default_cluster
ENGINE = Atomic
COMMENT 'Clickh db on cluster by my_schema student';

/*
 * get system/cluster/host parameters
 */
--version
select version();
--cluster information
select * from system.clusters;
--show which replica and shard are on current host
select * from system.macros;
--list available dbs on current cluster:
select * from system.databases
--where name = 'my_schema'
;
--list available table engines:
select * from system.table_engines;
--list available tables at 'my_schema' db:
select table_name from information_schema.tables
where table_schema = 'my_schema';
--list available dictionaries at 'my_schema' db:
select database, name, status, last_successful_update_time, last_exception, type, comment
from system.dictionaries
where database = 'my_schema';

/*
 * 2. Создайте в своей базе данных интеграционную таблицу ch_plan_fact_ext
 * для доступа к данным витрины plan_fact_<YYYYMM> в системе Greenplum.
 * (используется движок PostgreSQL для подключения к БД Greenplum)
 */
DROP TABLE my_schema.ch_plan_fact_ext;
--DOES NOT WORK THE WAY IT SHOWN IN PRACTICA VIDEO:
--EXECUTE HIGHLIGHTED WHOLE QUERY WITH [; ORDER BY ...] PART
CREATE TABLE my_schema.ch_plan_fact_ext (
	"Код региона" TEXT,
	"Код товарного направления" Int32,
	"Код канала сбыта" Int32,
	"План" Int32,
	"Факт" Int32,
	"% выполнения плана" DECIMAL(19, 4),
	"Самый продаваемый" Int32
)
ENGINE = PostgreSQL('255.255.255.203:5432', 'adb', 'plan_fact_202101', 'my_schema', 'my_password', 'my_schema');
ORDER BY "Регион";
--check it's working
SELECT * FROM my_schema.ch_plan_fact_ext LIMIT 10;
SELECT count(*) FROM my_schema.ch_plan_fact_ext;

/*
 * 3. Создайте следующие словари для доступа к данным таблиц системы Greenplum
 */
/*
 * 1. ch_price_dict
 */
DROP DICTIONARY my_schema.ch_price_dict;

CREATE OR REPLACE DICTIONARY my_schema.ch_price_dict ON CLUSTER default_cluster (
	material Int32, --PrimKey
	region VARCHAR, --PrimKey
	dist_chan Int32, --PrimKey
	price Int32
)
PRIMARY KEY material, region, dist_chan
SOURCE(POSTGRESQL(
	host '255.255.255.203'
	port 5432
	db 'adb'
	table 'my_schema.price'
    user 'my_schema'
    password 'my_password'
))
LAYOUT(COMPLEX_KEY_HASHED_ARRAY())
LIFETIME(0)
COMMENT 'ch_price_dict on cluster by my_schema student';
/*
 * 2. ch_chanel_dict
 */
DROP DICTIONARY my_schema.ch_chanel_dict ON CLUSTER default_cluster;

CREATE OR REPLACE DICTIONARY my_schema.ch_chanel_dict ON CLUSTER default_cluster (
	distr_chan Int32, --PrimKey
	txtsh TEXT
)
PRIMARY KEY distr_chan
SOURCE(POSTGRESQL(
	host '255.255.255.203'
	port 5432
	db 'adb'
	table 'my_schema.chanel'
    user 'my_schema'
    password 'my_password'
))
LAYOUT(HASHED())
LIFETIME(0)
COMMENT 'ch_chanel_dict on cluster by my_schema student';
/*
 * 3. ch_product_dict
 */
DROP DICTIONARY my_schema.ch_product_dict ON CLUSTER default_cluster;

CREATE OR REPLACE DICTIONARY my_schema.ch_product_dict ON CLUSTER default_cluster (
	material Int32, --PrimKey
	asgrp VARCHAR,
	brand Int32,
	matcateg VARCHAR,
	matdirec Int32,
	txt TEXT
)
PRIMARY KEY material
SOURCE(POSTGRESQL(
	host '255.255.255.203'
	port 5432
	db 'adb'
	table 'my_schema.product'
    user 'my_schema'
    password 'my_password'
))
LAYOUT(HASHED())
LIFETIME(0)
COMMENT 'ch_product_dict on cluster by my_schema student';
/*
 * 4. ch_region_dict
 */
DROP DICTIONARY my_schema.ch_region_dict ON CLUSTER default_cluster;

CREATE OR REPLACE DICTIONARY my_schema.ch_region_dict ON CLUSTER default_cluster (
	region VARCHAR, --PrimKey
	txt TEXT
)
PRIMARY KEY region
SOURCE(POSTGRESQL(
	host '255.255.255.203'
	port 5432
	db 'adb'
	table 'my_schema.region'
    user 'my_schema'
    password 'my_password'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(0)
COMMENT 'ch_region_dict on cluster by my_schema student';

--list available dictionaries at 'my_schema' db:
select database, name, status, last_successful_update_time, last_exception, type, comment
from system.dictionaries
where database = 'my_schema';
--load and check them
select dictGet('my_schema.ch_price_dict', 'price', tuple(128047, 'R001', 1));
select dictGet('my_schema.ch_chanel_dict', 'txtsh', 2);
select dictGet('my_schema.ch_product_dict', 'txt', 1428824);
select dictGet('my_schema.ch_region_dict', 'txt', 'R002');

/*
 * 4. Создайте реплицированные таблицы ch_plan_fact на всех хостах кластера.
 * Создайте распределённую таблицу ch_plan_fact_distr, выбрав для неё корректный ключ шардирования.
 * Вставьте в неё все записи из таблицы  ch_plan_fact_ext.
 */
/*
 * реплицированные таблицы ch_plan_fact на всех хостах кластера:
 */
DROP TABLE my_schema.ch_plan_fact ON CLUSTER default_cluster SYNC;

CREATE TABLE my_schema.ch_plan_fact ON CLUSTER default_cluster
AS my_schema.ch_plan_fact_ext
ENGINE = ReplicatedMergeTree('/click/my_schema.ch_plan_fact/{shard}', '{replica}')
ORDER BY "Код региона";

--check ch_plan_fact
select table_name, ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_schema = 'my_schema' AND table_name = 'ch_plan_fact'
order by ordinal_position;

select count(*) from my_schema.ch_plan_fact;

select * from my_schema.ch_plan_fact limit 10;

/*
 * распределённая таблица ch_plan_fact_distr
 */
DROP TABLE my_schema.ch_plan_fact_distr ON CLUSTER default_cluster SYNC;

CREATE TABLE my_schema.ch_plan_fact_distr ON CLUSTER default_cluster
AS my_schema.ch_plan_fact
ENGINE = Distributed('default_cluster', 'my_schema', 'ch_plan_fact', "Код канала сбыта");

--check ch_plan_fact_distr
select table_name, ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
from INFORMATION_SCHEMA.COLUMNS
where table_schema = 'my_schema' AND table_name = 'ch_plan_fact_distr'
order by ordinal_position;

select count(*) from my_schema.ch_plan_fact_distr;

select * from my_schema.ch_plan_fact_distr limit 10;

/*
 * Вставка в ch_plan_fact_distr все записи из таблицы ch_plan_fact_ext
 */
INSERT INTO my_schema.ch_plan_fact_distr
SELECT * FROM my_schema.ch_plan_fact_ext;






