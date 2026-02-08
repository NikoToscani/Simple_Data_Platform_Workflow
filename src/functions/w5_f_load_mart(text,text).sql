/*
 * 2. Создайте пользовательскую функцию в схеме std <номер студента> для расчёта витрины,
 * которая будет содержать результат выполнения плана продаж.
 * 3. Создайте представление (VIEW) на созданной ранее витрине в схеме std <номер студента>.
 */

/*
 * функция для расчета месячной витрины "План-факт" и создания представления
 */
drop function my_schema.f_load_mart(text, text);

CREATE OR REPLACE FUNCTION
my_schema.f_load_mart(p_date_format text, p_month text)
    RETURNS int8
    LANGUAGE plpgsql
    SECURITY definer
    VOLATILE
AS $$
DECLARE
    v_date_format text;
    v_month text;
    v_date date;
    v_start_date date;
    v_end_date date;
    v_mart_name text;
    v_table_prefix text := 'my_schema.plan_fact_';
    v_view_name text := 'my_schema.v_plan_fact';
    v_sql text;
    v_return int8;
BEGIN
    PERFORM my_schema.f_write_log(
		p_log_type := 'INFO',
		p_log_message := 'Start f_load_mart(text,text)',
		p_location := 'f_load_mart(text,text)'
	);

    v_date_format = upper(trim(translate(p_date_format, ';''','')));
    v_month = upper(trim(translate(p_month, ';''','')));
    --convert input f/ text to date
    v_date = to_date(v_month, v_date_format);
    --round date to month for this specific mart
    v_start_date = date_trunc('month', v_date)::date;
    --round and create upper range for more common case
    --but for this specific mart - strictly for 1 month
    v_end_date = (date_trunc('month', v_date)
                 + interval'1 month' - interval'1 day')::date;

	EXECUTE 'DROP VIEW IF EXISTS '||v_view_name;
    v_mart_name = v_table_prefix||to_char(v_start_date, 'YYYYMM');
    EXECUTE 'DROP TABLE IF EXISTS '||v_mart_name;
    --запрос для выбора требуемых столбцов, строк и данных будущей витрины
    v_sql = '
	with
	ranged_sales as (
		select
			*
		from
			sales
		where date between '''||v_start_date||'''::date and '''||v_end_date||'''::date
	),
	sum_qty_sales as (
		select
			*,
			sum(quantity) over (
				partition by region, material
			) as sum_qty
		from
			ranged_sales
	),
	sales_w_best_sale as (
		select
			*,
			first_value(material) over (
				partition by region
				order by sum_qty DESC, material
			) as best_selling
		from
			sum_qty_sales
	),
	monthly_plan as (
		select
			*
		from
			plan
		where date between date_trunc(''month'', '''||v_start_date||'''::date)::date and
						   date_trunc(''month'', '''||v_start_date||'''::date)::date + interval''1 month'' - interval''1 day''
	)
	select
		s.region as "Код региона",
		prod.matdirec as "Код товарного направления",
		s.dist_chan as "Код канала сбыта",
		pl.quantity as "План",
		s.quantity as "Факт",
		round(s.quantity * 1.0 / pl.quantity * 100) as "% выполнения плана",
		s.best_selling as "Самый продаваемый"
	from
		sales_w_best_sale s
		left outer join my_schema.product prod on s.material = prod.material
		left outer join monthly_plan pl on prod.matdirec = pl.matdirec and
										   s.dist_chan = pl.dist_chan and
										   s.region = pl.region
	';
    EXECUTE '
    CREATE TABLE '||v_mart_name||'
    WITH (
        appendonly=true,
        orientation=column,
        compresstype=zstd,
        compresslevel=1
    ) AS '||v_sql||'
    DISTRIBUTED RANDOMLY';
    GET DIAGNOSTICS v_return = ROW_COUNT;

	--создание представления
	EXECUTE '
	CREATE VIEW '||v_view_name||' AS
		SELECT
			m."Код региона",
			r.txt AS "Регион",
			m."Код товарного направления",
			m."Код канала сбыта",
			c.txtsh AS "Канал сбыта",
			m."% выполнения плана",
			m."Самый продаваемый",
			prod.brand AS "Код бренда самого продаваемого",
			prod.txt AS "Описание самого продаваемого",
			pr.price AS "Цена самого продаваемого в регионе"
		FROM
			'||v_mart_name||' as m
			left outer join my_schema.region r on m."Код региона" = r.region
			left outer join my_schema.chanel c on m."Код канала сбыта" = c.distr_chan
			left outer join my_schema.product prod on m."Самый продаваемый" = prod.material
			left outer join my_schema.price pr on prod.material = pr.material AND
											      r.region = pr.region
	';

    PERFORM my_schema.f_write_log(
		p_log_type := 'INFO',
		p_log_message := v_return||' rows inserted',
		p_location := 'f_load_mart(text,text)'
	);
    PERFORM my_schema.f_write_log(
		p_log_type := 'INFO',
		p_log_message := 'End f_load_mart(text,text)',
		p_location := 'f_load_mart(text,text)'
	);

    RETURN v_return;
END;
$$
EXECUTE ON ANY;

--создание витрины за период 01 января 2021 - 31 января 2021
select my_schema.f_load_mart('MM/YYYY', '01/2021');

--/*
-- * How it works
-- */
----get tables
----select count(*)
--select table_name
--from information_schema.tables
--where table_schema = 'my_schema';
--
----get views
--SELECT viewname
--FROM pg_views
--WHERE schemaname = 'my_schema'
--ORDER BY viewname;
--
--select my_schema.f_load_mart('MM/YYYY', '01/2021');
--
----check the mart
----size
--select pg_size_pretty(pg_total_relation_size('my_schema.plan_fact_202101')) as size;
--
----table data structure
--select table_name, ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default
--from INFORMATION_SCHEMA.COLUMNS
--where table_name = 'plan_fact_202101'
--order by ordinal_position;
--	
--select count(*) from my_schema.plan_fact_202101;
--select * from my_schema.plan_fact_202101 limit 10;
--
--select count(*) from my_schema.v_plan_fact;
--select * from my_schema.v_plan_fact limit 10;
--
----get distribution by serments of available table (table_name)
--select gp_segment_id, count(*) from my_schema.plan_fact_202101 group by gp_segment_id;
----skew coefficient general (replace 'schema_name.table_name' to proper ones)
--select (gp_toolkit.gp_skew_coefficient('my_schema.plan_fact_202101'::regclass)).skccoeff;
--
----existiong partitions
--select
--    partitiontablename, partitionrangestart, partitionrangeend, partitionboundary
--from pg_partitions
--where schemaname = 'my_schema' AND tablename = 'plan_fact_202101'
--order by partitionrangestart;
----distribution by partitions
--SELECT tableoid::regclass AS partition_name, COUNT(*) AS record_count
--FROM my_schema.plan_fact_202101
--GROUP BY tableoid
--ORDER BY partition_name;
--
--
----drop the mart
--drop view my_schema.v_plan_fact;
--drop table my_schema.plan_fact_202101;





