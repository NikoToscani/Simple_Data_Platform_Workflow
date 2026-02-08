/*
 * 2. Создайте датасет (используя свое соединение) ss_plan_fact
 * с помощью собственного SQL запроса к таблице в Clickhouse и
 * с использованием функций для словарей-справочников.
 * SQL запрос должен формировать аналогичное представление, 
 * как в Greenplum (v_plan_fact), с результатами выполнения плана продаж,
 * текстами для кодов и информацией о самом продаваемом товаре в регионе.
 */
SELECT
	pf."Код региона",
	dictGet('my_schema.ch_region_dict', 'txt', pf."Код региона") AS "Регион",
	pf."Код товарного направления",
	pf."Код канала сбыта",
	dictGet('my_schema.ch_chanel_dict', 'txtsh', pf."Код канала сбыта") AS "Канал сбыта",
	pf."% выполнения плана",
	pf."Самый продаваемый",
	dictGet('my_schema.ch_product_dict', 'brand', pf."Самый продаваемый") AS "Код бренда самого продаваемого",
	dictGet('my_schema.ch_product_dict', 'txt', pf."Самый продаваемый") AS "Описание самого продаваемого",
	coalesce(
	  dictGetOrNull('my_schema.ch_price_dict', 'price', tuple(pf."Самый продаваемый", pf."Код региона", pf."Код канала сбыта")),
	  dictGetOrNull('my_schema.ch_price_dict', 'price', tuple(pf."Самый продаваемый", pf."Код региона", 1)),
	  dictGetOrNull('my_schema.ch_price_dict', 'price', tuple(pf."Самый продаваемый", pf."Код региона", 2))
	) AS "Цена самого продаваемого в регионе",
	CASE pf."Код региона"
		WHEN 'R001' THEN 'RU-MOW'
		WHEN 'R002' THEN 'RU-SPE'
		WHEN 'R003' THEN 'RU-SAR'
		ELSE 'RU-TA'
	END AS iso_reg_code
FROM
	my_schema.ch_plan_fact_distr as pf





