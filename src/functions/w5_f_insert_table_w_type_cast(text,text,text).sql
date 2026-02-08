/*
 * функция для вставки данных с преведение типов:
 *	функция предполагает, что ordinal position и количество полей совпадает
 */
drop function my_schema.f_insert_table_w_type_cast(text, text, text);

CREATE OR REPLACE FUNCTION
my_schema.f_insert_table_w_type_cast(
    p_table_from text, p_table_to text, p_where text
)
    RETURNS int8
    LANGUAGE plpgsql
    SECURITY definer
    VOLATILE
AS $$
DECLARE
    v_table_from    text;
    v_table_wo_schema_from	text;
    v_schema_from   text;
    v_table_to      text;
    v_table_wo_schema_to	text;
    v_schema_to     text;
    v_cols_select 	text;
	v_sql			text;
    v_cnt           int8;
BEGIN
    v_table_from = lower(trim(translate(p_table_from, ';/''','')));
	v_schema_from = left(v_table_from, position('.' in v_table_from) - 1);
    v_table_wo_schema_from = right(v_table_from, length(v_table_from) - position('.' in v_table_from));

    v_table_to = lower(trim(translate(p_table_to, ';/''','')));
	v_schema_to = left(v_table_to, position('.' in v_table_to) - 1);
    v_table_wo_schema_to = right(v_table_to, length(v_table_to) - position('.' in v_table_to));

    v_cnt = 0;

    --список позиций полей v_table_to и полей с приведением типов v_table_from
    WITH src AS (
        SELECT
            attnum,
            attname AS src_col
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = v_schema_from
        AND c.relname = v_table_wo_schema_from
        AND attnum > 0
        AND NOT attisdropped
    ),
	tgt AS (
        SELECT
            attnum,
            pg_catalog.format_type(atttypid, atttypmod) AS tgt_type
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = v_schema_to
        AND c.relname = v_table_wo_schema_to
        AND attnum > 0
        AND NOT attisdropped
    )
	SELECT
	    string_agg(
	        format('%I.%I::%s', v_table_wo_schema_from, src.src_col, tgt.tgt_type),
	        ', ' ORDER BY tgt.attnum
	    )
	INTO
	    v_cols_select
	FROM tgt
	JOIN src USING (attnum);
		
    --загрузка данных
	EXECUTE 'INSERT INTO '||v_table_to||' SELECT '||v_cols_select||' FROM '||v_table_from||' WHERE '||p_where;

    GET DIAGNOSTICS v_cnt = ROW_COUNT;
	RAISE NOTICE 'f_insert_table_w_type_cast is about to finish';
    RETURN v_cnt;
END;
$$
EXECUTE ON ANY;

/*
 * How it works
 */
--select * from sales limit 10;
--select count(*) from sales limit 10;
--select * from ext_sales limit 10;
--select count(*) from ext_sales limit 10;
--
--select my_schema.f_insert_table_w_type_cast('my_schema.ext_sales', 'my_schema.sales', '1=1');
--
--truncate table sales;
--
--
--select * from plan limit 10;
--select count(*) from plan limit 10;
--select * from ext_plan limit 10;
--select count(*) from ext_plan limit 10;
--
--select my_schema.f_insert_table_w_type_cast('my_schema.ext_plan', 'my_schema.plan', '1=1');
--
--truncate table plan;






