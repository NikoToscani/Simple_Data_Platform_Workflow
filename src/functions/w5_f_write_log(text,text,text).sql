/*
 * Объекты обеспечения логирования и функция логирования
 */
--таблица логов
drop table my_schema.logs;

CREATE TABLE my_schema.logs (
    log_id int8 NOT NULL,
    log_timestamp timestamp NOT NULL DEFAULT now(),
    log_type text NOT NULL,
    log_msg text NOT NULL,
    log_location text NULL,
    is_error bool NULL,
    log_user text NULL DEFAULT "current_user"(),
    CONSTRAINT pk_log_id PRIMARY KEY (log_id)
)
DISTRIBUTED BY (log_id);

--последовательность для таблицы логов
drop sequence my_schema.log_id_seq;

CREATE SEQUENCE my_schema.log_id_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775800
    START 1;

--функция записи логов
drop function my_schema.f_write_log(text, text, text);

CREATE OR REPLACE FUNCTION my_schema.f_write_log(
    p_log_type text, p_log_message text, p_location text
)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$
DECLARE
    v_log_type text;
    v_log_message text;
    v_sql text;
    v_location text;
    v_res text;
BEGIN
    --Check message type
    v_log_type = upper(p_log_type);
    v_location = lower(p_location);
  
    IF v_log_type not in ('ERROR', 'INFO') THEN
        RAISE EXCEPTION 'Illegal log type! Use one of: ERROR, INFO';
    END IF;

    RAISE NOTICE '%: %: <%> Location[%]', clock_timestamp(), v_log_type, p_log_message, v_location;

    v_log_message := replace(p_log_message, '''', '''''');
  
    v_sql := 'INSERT INTO my_schema.logs(log_id, log_type, log_msg, log_location, is_error, log_timestamp, log_user) 
              VALUES ( ' || nextval('my_schema.log_id_seq')|| ',
                     ''' || v_log_type || ''',
                       ' || coalesce('''' || v_log_message || '''', '''empty''')|| ',
                       ' || coalesce('''' || v_location || '''','null')|| ',
                       ' || CASE WHEN v_log_type = 'ERROR' THEN TRUE ELSE FALSE END || ',
                           current_timestamp,current_user);';
    
    RAISE NOTICE 'INSERT SQL IS: %', v_sql;
    --dblink for fdw server
    v_res := dblink('postgresql://255.255.255.203:5432/adb?user=my_schema&password=my_password', v_sql); 
END;
$$
EXECUTE ON ANY;

--/*
-- * How it works
-- */
----list existing tables
--SELECT table_name FROM information_schema.tables
--WHERE table_schema = 'my_schema';
----list existing sequences
--SELECT sequence_schema, sequence_name FROM information_schema.sequences
--WHERE sequence_schema = 'my_schema' ORDER BY sequence_name;
----list existing functions
--SELECT * FROM information_schema.routines
--WHERE routine_type = 'FUNCTION' AND routine_schema = 'my_schema';
--
--SELECT * FROM my_schema.logs limit 20;
--
--select my_schema.f_write_log('ERROR', 'End f_load_mart', 'f_load_mart(text,text)');
----or
--select my_schema.f_load_mart('MM/YYYY', '01/2021');
----query with error
--select my_schema.f_load_full('my_schema.ext_price', 'std15_177.price', '1=1', TRUE);
--select my_schema.f_load_full('my_schema.ext_price', 'my_schema.price', '1=1', TRUE);
--
--truncate table my_schema.logs;





