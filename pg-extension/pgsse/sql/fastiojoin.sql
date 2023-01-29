CREATE OR REPLACE PROCEDURE pgsse_fastjoin_init(prefix text)
 LANGUAGE plpgsql
AS $$
DECLARE

    create_te_sql text; 
    create_tc_sql text; 

BEGIN

    CALL pgsse_fastio_init(prefix || '_a');
    CALL pgsse_fastio_init(prefix || '_b');

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
        tw bytea,
        a_token bytea
        b_token bytea
    );
    ';
    
    EXECUTE format(create_te_sql, prefix || '_ab');
    EXECUTE format(create_tc_sql, prefix || '_ab');

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastiojoin_drop(prefix text)
 LANGUAGE plpgsql
AS $$
DECLARE

    drop_te_sql text; 
    drop_tc_sql text; 

BEGIN

    CALL pgsse_fastio_drop(prefix || '_a');
    CALL pgsse_fastio_drop(prefix || '_b');

    drop_te_sql = '
    DROP TABLE IF EXISTS %I_te
    ';

    drop_tc_sql = '
    DROP TABLE IF EXISTS %I_tc;
    ';
    
    EXECUTE format(drop_te_sql, prefix || '_ab');
    EXECUTE format(drop_tc_sql, prefix || '_ab');

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastiojoin_update(prefix text, a_token bytea, b_token bytea, ab_token bytea)
 LANGUAGE plpgsql
AS $$
DECLARE

    u bytea; 
    e bytea; 

BEGIN

    CALL pgsse_fastio_update(prefix || '_a', a_token);
    CALL pgsse_fastio_update(prefix || '_b', b_token);

    u = substring(token from 1 for 64);
    e = substring(token from 65 for 64);

    EXECUTE format('INSERT INTO %I_te VALUES ($1, $2)', prefix || '_ab') USING u, e;

END;
$$
;
