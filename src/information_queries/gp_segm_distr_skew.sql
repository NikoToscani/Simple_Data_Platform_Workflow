/*
 * check gp configuration (master, segments primes and mirrors)
 */
select * from gp_segment_configuration;

/*
 * get distribution by serments of available table (table_name)
 */
select gp_segment_id, count(*) from table_name group by gp_segment_id;
/*
 * sales
 */
select gp_segment_id, count(*) from sales group by gp_segment_id;
/*
 * plan
 */
select gp_segment_id, count(*) from plan group by gp_segment_id;


/*
 * skew coefficient general (replace 'schema_name.table_name' to proper ones)
 */
select (gp_toolkit.gp_skew_coefficient('schema_name.table_name'::regclass)).skccoeff;
/*
 * sales
 */
select (gp_toolkit.gp_skew_coefficient('my_schema.sales'::regclass)).skccoeff;
/*
 * plan
 */
select (gp_toolkit.gp_skew_coefficient('my_schema.plan'::regclass)).skccoeff;






