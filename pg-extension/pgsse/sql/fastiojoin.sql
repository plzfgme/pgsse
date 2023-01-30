CREATE OR REPLACE PROCEDURE pgsse_fastiojoin_core_init(prefix text)
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
        token bytea
    );
    ';
    
    EXECUTE format(create_te_sql, prefix);
    EXECUTE format(create_tc_sql, prefix);

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastiojoin_core_drop(prefix text)
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

CREATE OR REPLACE PROCEDURE pgsse_fastiojoin_core_update(prefix text, token bytea)
 LANGUAGE plpgsql
AS $$
DECLARE

    u bytea; 
    e bytea; 

BEGIN

    u = substring(token from 1 for 32);
    e = substring(token from 33 for 96);

    EXECUTE format('INSERT INTO %I_te VALUES ($1, $2)', prefix) USING u, e;

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastiojoin_side_init(prefix text)
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
        a_ids bigint[],
        b_ids bigint[]
    );
    ';
    
    EXECUTE format(create_te_sql, prefix);
    EXECUTE format(create_tc_sql, prefix);

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastiojoin_side_drop(prefix text)
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

CREATE OR REPLACE PROCEDURE pgsse_fastiojoin_side_update(prefix text, token bytea)
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

CREATE OR REPLACE PROCEDURE pgsse_fastjoin_init(prefix text)
 LANGUAGE plpgsql
AS $$
BEGIN

    CALL pgsse_fastiojoin_core_init(prefix || '_core');
    CALL pgsse_fastiojoin_side_init(prefix || '_side');

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastiojoin_drop(prefix text)
 LANGUAGE plpgsql
AS $$
BEGIN

    CALL pgsse_fastiojoin_core_drop(prefix || '_core');
    CALL pgsse_fastiojoin_side_drop(prefix || '_side');

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastiojoin_update(prefix text, core_token bytea, side_token bytea)
 LANGUAGE plpgsql
AS $$
BEGIN

    CALL pgsse_fastiojoin_core_update(prefix || '_core', core_token);
    CALL pgsse_fastiojoin_side_update(prefix || '_side', side_token);

END;
$$
;
