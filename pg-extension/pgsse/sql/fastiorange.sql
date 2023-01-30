CREATE OR REPLACE PROCEDURE pgsse_fastiorange_init(prefix text)
 LANGUAGE plpgsql
AS $$
BEGIN

    CALL pgsse_fastio_init(prefix);

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastiorange_drop(prefix text)
 LANGUAGE plpgsql
AS $$
BEGIN

    CALL pgsse_fastio_drop(prefix text);

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastiorange_update(prefix text, token bytea[])
 LANGUAGE plpgsql
AS $$
DECLARE

    fastio_token bytea;
    
BEGIN

    FOREACH fastio_token IN ARRAY token
    LOOP
        CALL pgsse_fastio_update(prefix, fastio_token);
    END LOOP;

END;
$$
;