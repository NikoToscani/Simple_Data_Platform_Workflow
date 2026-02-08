/*
 * ext_sales
 */
--drop ext_sales
drop external table my_schema.ext_sales;
--creation external table sales
--postgres URL: jdbc:postgresql://255.255.255.212:5432/postgres  --default used port 8080 if not specified
create readable external table my_schema.ext_sales(
	check_nm VARCHAR, --PrimKey
	check_pos VARCHAR, --PrimKey
	material VARCHAR,
	region VARCHAR,
	distr_chan VARCHAR,
	quantity INT,
	"date" DATE
)
location ('pxf://gp.sales?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://255.255.255.212:5432/postgres&USER=login&PASS=password')
format 'CUSTOM' (formatter='pxfwritable_import');

--postgres URL: jdbc:postgresql://255.255.255.212:5432/postgres  --default used port 8080 if not specified
--create readable external table ext_sales(
--	like sales
--)
--location ('pxf://gp.sales?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://255.255.255.212:5432/postgres&USER=login&PASS=password')
--format 'CUSTOM' (formatter='pxfwritable_import');

/*
 * ext_plan
 */
--drop ext_plan
drop external table my_schema.ext_plan;
--creation external table plan
create readable external table my_schema.ext_plan(
	"date" DATE, --PrimKey
	region VARCHAR, --PrimKey
	matdirec VARCHAR, --PrimKey
	distr_chan VARCHAR,  --PrimKey
    quantity INT
)
location ('pxf://gp.plan?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://255.255.255.212:5432/postgres&USER=login&PASS=password')
format 'CUSTOM' (formatter='pxfwritable_import');

/*
 * ext_price
 */
--drop ext_price
drop external table my_schema.ext_price;
--creation external table plan
create readable external table my_schema.ext_price(
	material INT, --PrimKey
	region VARCHAR, --PrimKey
	distr_chan INT, --PrimKey
	price INT
)
location ('gpfdist://172.16.128.74:8080/w4_extern_sys_integration/price.csv')
format 'CSV' (header delimiter as ';')
segment reject limit 1 percent;

/*
 * ext_product
 */
--drop ext_product
drop external table my_schema.ext_product;
--creation external table plan
create readable external table my_schema.ext_product(
	material INT, --PrimKey
	asgrp VARCHAR,
	brand INT,
	matcateg VARCHAR,
	matdirec INT,
	txt TEXT
)
location ('gpfdist://172.16.128.74:8080/w4_extern_sys_integration/product.csv')
format 'CSV' (header delimiter as ';');

/*
 * ext_chanel
 */
--drop ext_chanel
drop external table my_schema.ext_chanel;
--creation external table plan
create readable external table my_schema.ext_chanel(
	distr_chan INT, --PrimKey
	txtsh TEXT
)
location ('gpfdist://172.16.128.74:8080/w4_extern_sys_integration/chanel.csv')
format 'CSV' (header delimiter as ';');

/*
 * ext_region
 */
--drop ext_region
drop external table my_schema.ext_region;
--creation external table plan
create readable external table my_schema.ext_region(
	region VARCHAR, --PrimKey
	txt TEXT
)
location ('gpfdist://172.16.128.74:8080/w4_extern_sys_integration/region.csv')
format 'CSV' (header delimiter as ';');





