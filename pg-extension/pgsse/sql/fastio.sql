CREATE OR REPLACE PROCEDURE pgsse_fastio_init(prefix text)
 LANGUAGE plpgsql
AS $$
DECLARE

    create_te_sql text; 
    create_tc_sql text; 

BEGIN

    create_te_sql = '
    CREATE TABLE IF NOT EXISTS %I_te
    (
        u bytea PRIMARY KEY,
        e bytea
    );
    ';

    create_tc_sql = '
    CREATE TABLE IF NOT EXISTS %I_tc
    (
        tw bytea PRIMARY KEY,
        ids bytea[]
    );
    ';
    
    EXECUTE format(create_te_sql, prefix);
    EXECUTE format(create_tc_sql, prefix);

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastio_drop(prefix text)
 LANGUAGE plpgsql
AS $$
DECLARE

    drop_te_sql text; 
    drop_tc_sql text; 

BEGIN

    drop_te_sql = '
    DROP TABLE IF EXISTS %I_te
    ';

    drop_tc_sql = '
    DROP TABLE IF EXISTS %I_tc;
    ';
    
    EXECUTE format(drop_te_sql, prefix);
    EXECUTE format(drop_tc_sql, prefix);

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastio_update(prefix text, token bytea)
 LANGUAGE plpgsql
AS $$
DECLARE

    u bytea; 
    e bytea; 

BEGIN

    u = substring(token from 1 for 32);
    e = substring(token from 33 for 32);

    EXECUTE format('INSERT INTO %I_te VALUES ($1, $2)', prefix) USING u, e;

END;
$$
;
