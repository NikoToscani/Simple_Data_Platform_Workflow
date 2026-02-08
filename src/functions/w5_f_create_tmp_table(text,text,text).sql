/*
 * функция для создания псевдо-временной таблицы:
 */
DROP FUNCTION my_schema.f_create_tmp_table(text, text, text);

CREATE OR REPLACE FUNCTION
my_schema.f_create_tmp_table(p_table_name text, p_prefix text, p_suffix text)
    RETURNS text
    LANGUAGE plpgsql
    SECURITY definer
    VOLATILE
AS $$
DECLARE
    v_table_name        text;
    v_table_wo_schema_name	text;
    v_schema_name       text;
    v_tmp_t_name        text;
    v_storage_param     text;
    v_table_oid         int4;
    v_dist_key          text;
    v_sql               text;
BEGIN
    v_table_name = lower(trim(translate(p_table_name, ';/''','')));
	v_schema_name = left(v_table_name, position('.' in v_table_name) - 1);
    v_table_wo_schema_name = right(v_table_name, length(v_table_name) - position('.' in v_table_name));
    v_tmp_t_name = v_schema_name||'.'||p_prefix||v_table_wo_schema_name||p_suffix;
    -- get table storage properties
    SELECT coalesce('with (' || array_to_string(reloptions, ', ') || ')','')
    INTO v_storage_param
    FROM pg_class  
    WHERE oid = v_table_name::regclass;
    -- get distribution key
    SELECT c.oid INTO v_table_oid
    FROM pg_class c INNER JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname||'.'||c.relname = v_table_name
    LIMIT 1;
    IF v_table_oid = 0 OR v_table_oid IS NULL THEN
        v_dist_key = 'DISTRIBUTED RANDOMLY';
    ELSE
        v_dist_key = pg_get_table_distributedby(v_table_oid);
    END IF;

    v_sql := 'CREATE TABLE '||v_tmp_t_name||' (like '||v_table_name||') '
    ||v_storage_param||''||v_dist_key||';';
    EXECUTE v_sql;
	RAISE NOTICE 'f_create_tmp_table is about to finish';
    RETURN v_tmp_t_name;
END;
$$
EXECUTE ON ANY;

--/*
-- * How it works
-- */
----check current tables available in schema
--select table_name
--from information_schema.tables
--where table_schema = 'my_schema' and table_name like '%sales%';
--select count(*)
--from information_schema.tables
--where table_schema = 'my_schema' and table_name like '%sales%';
--
---- create temp table of sales
--select my_schema.f_create_tmp_table('my_schema.sales', 'tmp_', '_01_13_1306');
--
--drop table my_schema.tmp_sales_01_13_1306;





