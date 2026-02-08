/*
 * check available functions for schema_name
 */
SELECT
    *
FROM
    information_schema.routines
WHERE
    routine_type = 'FUNCTION'
    AND routine_schema = 'schema_name';
/*
 * my_schema
 */
SELECT *
FROM information_schema.routines
WHERE routine_type = 'FUNCTION'
AND routine_schema = 'my_schema';
 



