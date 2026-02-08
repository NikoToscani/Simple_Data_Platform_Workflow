/*
 * 1. Создайте 2 пользовательские функции в схеме std <номер студента>
 * для загрузки данных в созданные на 2-ом уроке таблицы:
 */

/*
 * функция для DELTA PARTITION загрузки таблиц фактов:
 */
drop function my_schema.f_load_delta_partitions(text, text, text, date, date);

CREATE OR REPLACE FUNCTION
my_schema.f_load_delta_partitions(
    p_table_from text, p_table_to text, p_partition_key text,
    p_start_date date, p_end_date date
)
    RETURNS int8
    LANGUAGE plpgsql
    SECURITY definer
    VOLATILE
AS $$
DECLARE
    v_table_from    text;
    v_table_to      text;
    v_load_interval interval;
    v_start_date 	timestamp;
    v_end_date	  	timestamp;
    v_iter_date     timestamp;
    v_where         text;
    v_prt_table     text;
    v_cnt_prt       int8;
    v_cnt           int8;
    v_sql           text;
BEGIN
    v_table_from = lower(trim(translate(p_table_from, ';/''','')));
    v_table_to = lower(trim(translate(p_table_to, ';/''','')));
    v_cnt = 0;
    
    PERFORM my_schema.f_create_date_partition(
        p_table_name := v_table_to, p_partition_value := p_end_date
    );
    v_load_interval := '1 month'::interval;
    v_start_date := DATE_TRUNC('month', p_start_date);
    v_end_date := DATE_TRUNC('month', p_end_date) + v_load_interval;
    LOOP
        v_iter_date = v_start_date + v_load_interval;
        EXIT WHEN (v_iter_date > v_end_date);
        -- создание временной таблицы для получения данных из внешней таблицы
        v_prt_table = my_schema.f_create_tmp_table(
        	p_table_name := v_table_to, p_prefix := 'prt_',
			p_suffix := '_'||to_char(v_start_date, 'YYYYMMDD')
        );
        v_where = p_partition_key||'>='''||v_start_date||'''::timestamp AND '||
                    p_partition_key||'<'''||v_iter_date||'''::timestamp';
		-- заполенение временной таблицы данными из внешней по условию
		v_cnt_prt = my_schema.f_insert_table_w_type_cast(
			p_table_from := v_table_from, p_table_to := v_prt_table, p_where := v_where
		);
        v_cnt = v_cnt + v_cnt_prt;
		EXECUTE 'ALTER TABLE '||v_table_to||' EXCHANGE PARTITION FOR (DATE '''||v_start_date||'''::date)
				 WITH TABLE '||v_prt_table||' WITH VALIDATION';
        EXECUTE 'DROP TABLE '||v_prt_table;
        v_start_date = v_iter_date;
    END LOOP;
	RAISE NOTICE 'f_load_delta_partitions is about to finish';
    RETURN v_cnt;
END;
$$
EXECUTE ON ANY;

/*
 * How it works
 */
--/*
-- * SALES
-- */
--select * from sales limit 10;
--select count(*) from sales;
----row distribution in partitions of parent table
--select
--    partitiontablename::regclass as partition_name, 0 as record_count
--from pg_partitions
--where schemaname = 'my_schema' AND tablename = 'sales'
--union
--SELECT tableoid::regclass AS partition_name, COUNT(*) AS record_count
--FROM sales
--GROUP BY tableoid
--ORDER BY partition_name;
--
---- find max and min date at external tables
--select min(date), max(date) from ext_sales;
--select * from ext_sales limit 10;
--select count(*) from ext_sales limit 10;
--
--select my_schema.f_load_delta_partitions('my_schema.ext_sales', 'my_schema.sales', 'date', '2021-01-01', '2022-01-01');
--
--drop table sales;
----create table sales...;
--/*
-- * PLAN
-- */
--select * from plan limit 10;
--select count(*) from plan;
----row distribution in partitions of parent table
--select
--    partitiontablename::regclass as partition_name, 0 as record_count
--from pg_partitions
--where schemaname = 'my_schema' AND tablename = 'plan'
--union
--SELECT tableoid::regclass AS partition_name, COUNT(*) AS record_count
--FROM plan
--GROUP BY tableoid
--ORDER BY partition_name;
--
---- find max and min date at external tables
--select min(date), max(date) from ext_plan;
--select * from ext_plan limit 10;
--select count(*) from ext_plan limit 10;
--
--select my_schema.f_load_delta_partitions('my_schema.ext_plan', 'my_schema.plan', 'date', '2021-01-01', '2022-01-01');
--
--drop table plan;
----create table plan...;






