/*
 * функция для создания партиций в целевой таблице:
 */
DROP FUNCTION my_schema.f_create_date_partition(text, timestamp);

CREATE OR REPLACE FUNCTION
my_schema.f_create_date_partition(p_table_name text, p_partition_value timestamp)
    RETURNS void
    LANGUAGE plpgsql
    SECURITY definer
    VOLATILE
AS $$
DECLARE
    v_cnt_partitions    int;
    v_table_name        text;
    v_partition_end_sql text;
    v_partition_end     timestamp;
    v_interval          interval;
    v_ts_format         text := 'YYYY-MM-DD HH24:MI:SS';
BEGIN
    v_table_name = lower(trim(translate(p_table_name, ';/''','')));
    SELECT COUNT(*) INTO v_cnt_partitions
	FROM pg_partitions p
	WHERE p.schemaname||'.'||p.tablename = v_table_name;
    IF v_cnt_partitions > 1 THEN
        LOOP
            -- Получение параметра ('last partition end date'::date) последней партиции
            SELECT partitionrangeend INTO v_partition_end_sql
            FROM (
                SELECT p.*, RANK() OVER (ORDER BY partitionrank DESC) rnk FROM pg_partitions p
                WHERE p.partitionrank IS NOT NULL AND p.schemaname||'.'||p.tablename = v_table_name
            ) q
            WHERE rnk = 1;
            -- Конечная дата последней партиции
            EXECUTE 'SELECT '||v_partition_end_sql INTO v_partition_end;
            -- Если партиция уже есть, то EXIT из функции
            EXIT WHEN v_partition_end > p_partition_value;
            v_interval := '1 month'::interval;
            -- Вырез новой партиции из дефолтной партиции, если ее еще не существует
            EXECUTE 'ALTER TABLE '||v_table_name||' SPLIT DEFAULT PARTITION
            START ('||v_partition_end_sql||') END ('''||to_char(v_partition_end+v_interval, v_ts_format)||'''::timestamp)';
        END LOOP;
    END IF;
	RAISE NOTICE 'f_create_date_partition is about to finish';
END;
$$
EXECUTE ON ANY;


/*
 * How it works
 */
--select
--    partitiontablename, partitionrangestart, partitionrangeend, partitionboundary
--from pg_partitions
--where schemaname = 'my_schema' AND tablename = 'sales'
--order by partitionrangestart;
--select count(*) from pg_partitions where schemaname = 'my_schema' and tablename = 'sales';
--
--select my_schema.f_create_date_partition('my_schema.sales', '2022-01-01');
--
--drop table sales;
----create table sales...;
--
--select
--    partitiontablename, partitionrangestart, partitionrangeend, partitionboundary
--from pg_partitions
--where schemaname = 'my_schema' AND tablename = 'plan'
--order by partitionrangestart;
--select count(*) from pg_partitions where schemaname = 'my_schema' and tablename = 'plan';
--
--select my_schema.f_create_date_partition('my_schema.plan', '2022-01-01');
--
--drop table plan;
----create table plan...;






