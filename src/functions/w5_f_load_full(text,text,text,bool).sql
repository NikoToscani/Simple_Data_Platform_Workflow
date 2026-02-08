/*
 * 1. Создайте 2 пользовательские функции в схеме std <номер студента> для загрузки данных в созданные на 2-ом уроке таблицы:
 */

/*
 * функция для FULL загрузки справочников:
 */
drop function my_schema.f_load_full(text, text, text, bool);

CREATE OR REPLACE FUNCTION
my_schema.f_load_full(
    p_table_from text, p_table_to text, p_where text, p_truncate_tgt bool
)
    RETURNS int8
    LANGUAGE plpgsql
    SECURITY definer
    VOLATILE
AS $$
DECLARE
    v_table_from text;
    v_table_to text;
    v_where text;
    v_cnt int8;
BEGIN
    PERFORM my_schema.f_write_log(
		p_log_type := 'INFO',
		p_log_message := 'Start f_load_full(text,text,text,bool)',
		p_location := 'f_load_full(text,text,text,bool)'
	);

    v_table_from = lower(trim(translate(p_table_from, ';/''','')));
    v_table_to = lower(trim(translate(p_table_to, ';/''','')));
    v_where = coalesce(p_where, '1=1');

    IF coalesce(p_truncate_tgt, FALSE) IS TRUE THEN
        EXECUTE 'TRUNCATE TABLE '||v_table_to;
		RAISE NOTICE 'Table % was truncated', v_table_to;
    END IF;

    EXECUTE 'INSERT INTO '||v_table_to||' SELECT * FROM '||v_table_from||' WHERE '||v_where;
    GET DIAGNOSTICS v_cnt = ROW_COUNT;
    RAISE NOTICE '% rows inserted from % to %', v_cnt, v_table_from, v_table_to;

    PERFORM my_schema.f_write_log(
		p_log_type := 'INFO',
		p_log_message := v_cnt||' rows inserted',
		p_location := 'f_load_full(text,text,text,bool)'
	);
    PERFORM my_schema.f_write_log(
		p_log_type := 'INFO',
		p_log_message := 'End f_load_full(text,text,text,bool)',
		p_location := 'f_load_full(text,text,text,bool)'
	);

    RETURN v_cnt;
--error handling
EXCEPTION
	WHEN SQLSTATE '3F000' THEN
	    PERFORM my_schema.f_write_log(
			p_log_type := 'ERROR',
			p_log_message := 'ERROR 3F000: 0 rows inserted',
			p_location := 'f_load_full(text,text,text,bool)'
		);
		RETURN 0;		
END;
$$
EXECUTE ON ANY;

--/*
-- * How it works
-- */
--select * from price limit 10;
--select count(*) from price limit 10;
--select count(*) from ext_price limit 10;
--
--select my_schema.f_load_full('my_schema.ext_price', 'my_schema.price', '1=1', TRUE);
--
--truncate table price;
--
--
--select * from product limit 10;
--select count(*) from product limit 10;
--select count(*) from ext_product limit 10;
--
--select my_schema.f_load_full('my_schema.ext_product', 'my_schema.product', '1=1', TRUE);
--
--truncate table product;
--
--
--select * from chanel limit 10;
--select count(*) from chanel limit 10;
--select count(*) from ext_chanel limit 10;
--
--select my_schema.f_load_full('my_schema.ext_chanel', 'my_schema.chanel', '1=1', TRUE);
--
--truncate table chanel;
--
--
--select * from region limit 10;
--select count(*) from region limit 10;
--select count(*) from ext_region limit 10;
--
--select my_schema.f_load_full('my_schema.ext_region', 'my_schema.region', '1=1', TRUE);
--
--truncate table region;




