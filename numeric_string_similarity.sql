CREATE OR REPLACE FUNCTION numeric_string_similarity(in a text, in b text, in threshold float8 default 0)
  RETURNS float8 AS 
$BODY$
DECLARE
    dist INT;
    maxlen INT;
    score FLOAT;
BEGIN

    INSERT INTO assignmentpreelogs(fntext, created) SELECT 'numeric_string_similarity ('||$1||','||$2||','||$3||')',now();

    IF a IS NULL OR b IS NULL THEN
        RETURN 0.0;
    END IF;

    -- Handle "12.0" vs "12" equivalence
    IF a ~ '\.0$' AND b = substring(a FROM 1 FOR length(a)-2) THEN
        RETURN 1.0;
    ELSIF b ~ '\.0$' AND a = substring(b FROM 1 FOR length(b)-2) THEN
        RETURN 1.0;
    END IF;

    -- Empty string handling
    IF length(a) = 0 AND length(b) = 0 THEN
        RETURN 1.0;
    END IF;

    dist := levenshtein(a, b);
    maxlen := GREATEST(length(a), length(b));
    score := 1 - (dist::FLOAT / maxlen);

    IF threshold = 0 OR score > threshold THEN
        RETURN score;
    ELSE
        RETURN 0.0;
    END IF;
END;
$BODY$
  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER