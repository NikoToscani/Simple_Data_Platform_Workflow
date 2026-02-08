/*
 * get system version
 */
select version();

/*
 * list gp configuration (master, segments primes and mirrors)
 */
select * from gp_segment_configuration;

/*
 * list all dbs at cluster
 */
select datname from pg_database;

/*
 * list all schemas at db
 */
select nspname from pg_catalog.pg_namespace;
select * from pg_catalog.pg_namespace;
select nspname AS schema_name, pg_catalog.pg_get_userbyid(nspowner) AS owner
from pg_catalog.pg_namespace;

/*
 * get search path - where system seeks for asked tables
 */
show search_path;

/*
 * get current and session user
 */
select current_user;
select session_user;

/*
 * get currently used (or default?) schema
 */
select current_schema();

/*
 * list available tables at your schema schema_name
 */
select table_name
from information_schema.tables
where table_schema = 'schema_name';
/*
 * information_schema - some metadata tables
 */
select table_name
from information_schema.tables
where table_schema = 'information_schema';
/*
 * pg_catalog - some metadata tables
 */
select table_name
from information_schema.tables
where table_schema = 'pg_catalog';
/*
 * my_schema
 */
select table_name from information_schema.tables
where table_schema = 'my_schema';

/*
 * list available sequences at your schema schema_name
 */
SELECT sequence_schema, sequence_name
FROM information_schema.sequences
WHERE sequence_schema = 'schema_name'
ORDER BY sequence_name;
/*
 * my_schema
 */
SELECT sequence_schema, sequence_name FROM information_schema.sequences
WHERE sequence_schema = 'my_schema' ORDER BY sequence_name;

/*
 * list views in schema schema_name
 */
SELECT viewname
FROM pg_views
WHERE schemaname = 'schema_name'
ORDER BY viewname;
/*
 * my_schema
 */
SELECT viewname FROM pg_views
WHERE schemaname = 'my_schema' ORDER BY viewname;

/*
 * list available indexes at your schema schema_name
 */
SELECT tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'schema_name' -- or your specific schema name
ORDER BY tablename, indexname;

/*
 * list available indexes at your schema and table table_name
 */
SELECT tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'schema_name' AND tablename = 'your_table_name' -- or your specific schema name
ORDER BY tablename, indexname;








