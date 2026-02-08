/*
 * vacuum
 * 
 * (marks for rewrite but not delete)
 * regular vacuum saves from facuum full
 */
vacuum table_name;

--vacuum full
/*
 * vacuum full
 * 
 * exclusively locks table
 * phisically moves residual rows in place of deleted ones
 * 
 * thats why sometimes
 * create table ... like ...; insert into ... select * ...; drop ...; alter
 * is faster
 * 
 * vacuum for AO-tables acts like vacuum full if bloat > 10%
 */
vacuum full table_name;

/*
 * bloat evaluation for HEAP
 */
select * from gp_toolkit.gp_bloat_diag WHERE bdirelname = 'your_table_name';
/*
 * bloat evaluation for AO
 */
select * from gp_toolkit.__gp_aovisimap_compaction_info('sales'::regclass);

select * from gp_toolkit.gp_bloat_diag;

select * from gp_toolkit.__gp_aovisimap_compaction_info('my_schema.sales'::regclass);

SELECT * FROM gp_toolkit.gp_bloat_expected_pages;




