CREATE OR REPLACE FUNCTION phase_similarity(in a text, in b text, in threshold float8 default 0.8)
  RETURNS float8 AS 
$BODY$
DECLARE
    a_int INT;
    b_int INT;
    a_match TEXT[];
    b_match TEXT[];
    ratio FLOAT;
BEGIN
    IF a IS NULL OR b IS NULL THEN
        RETURN 0.0;
    END IF;

    a := LOWER(TRIM(a));
    b := LOWER(TRIM(b));

    IF a = '' AND b = '' THEN
        RETURN 1.0;
    ELSIF a = '' OR b = '' THEN
        RETURN 0.0;
    END IF;

    -- Extract digits as arrays
    a_match := regexp_match(a, '\d+');
    b_match := regexp_match(b, '\d+');

    IF a_match IS NOT NULL THEN
        a_int := a_match[1]::INT;
    END IF;

    IF b_match IS NOT NULL THEN
        b_int := b_match[1]::INT;
    END IF;

    IF a_int IS NOT NULL AND b_int IS NOT NULL THEN
        RETURN CASE WHEN a_int = b_int THEN 1.0 ELSE 0.0 END;
    END IF;

    -- Fallback to trigram similarity if integers not present
    ratio := similarity(a, b);  -- Requires pg_trgm extension
    RETURN CASE WHEN ratio >= threshold THEN ratio ELSE 0.0 END;
END;
$BODY$
  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER