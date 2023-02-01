CREATE OR REPLACE PROCEDURE pgsse_fastiojoin_init(prefix text)
 LANGUAGE plpgsql
AS $$
BEGIN

    CALL pgsse_fastio64_init(prefix);

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastio64_drop(prefix text)
 LANGUAGE plpgsql
AS $$
BEGIN

    CALL pgsse_fastio_drop(prefix);

END;
$$
;

CREATE OR REPLACE PROCEDURE pgsse_fastio64_update(prefix text, token bytea)
 LANGUAGE plpgsql
AS $$
BEGIN

    CALL pgsse_fastio_update(prefix, token); 

END;
$$
;
