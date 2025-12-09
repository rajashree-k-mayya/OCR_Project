CREATE OR REPLACE FUNCTION numeric_similarity(in a text, in b text, in tolerance float8)
  RETURNS float8 AS 
$BODY$
DECLARE
    a_num FLOAT;
    b_num FLOAT;
    diff FLOAT;
BEGIN

    INSERT INTO assignmentpreelogs(fntext, created) SELECT 'numeric_similarity ('||$1||','||$2||','||$3||')',now();

    -- Try converting both inputs to numbers
    BEGIN
        a_num := a::FLOAT;
        b_num := b::FLOAT;
    EXCEPTION WHEN others THEN
        RETURN 0.0; -- If conversion fails, return 0
    END;

    diff := abs(a_num - b_num);

    -- Exact match
    IF diff = 0 THEN
        RETURN 1.0;
    END IF;

    -- Case 1: tolerance > 0 → use tolerance-based match
    IF tolerance > 0 THEN
        IF diff >= tolerance THEN
            RETURN 0.0; -- Outside tolerance
        ELSE
            RETURN 1.0 - (diff / tolerance);
        END IF;
    END IF;

    IF GREATEST(abs(a_num), abs(b_num)) = 0 THEN
        RETURN 1.0 - (diff / GREATEST(abs(a_num), abs(b_num)));
            RETURN 0.0;
        END IF;

    -- Case 2: tolerance = 0 → use relative difference percentage
    RETURN 1.0 - (diff / GREATEST(abs(a_num), abs(b_num)));

END;
$BODY$
  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER