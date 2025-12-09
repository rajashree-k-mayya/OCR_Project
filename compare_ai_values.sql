CREATE OR REPLACE FUNCTION compare_ai_values(in ground_truth text, in extracted text, in field_type text, in tolerance float8 default 0, in threshold float8 default 0)
  RETURNS float8 AS 
$BODY$
BEGIN

    --INSERT INTO assignmentpreelogs(fntext, created) SELECT 'compare_ai_values ('||$1||','||$2||','||$3::text||','||$4||','||$5||')',now();

    CASE field_type
        WHEN 'string' THEN
            RETURN numeric_string_similarity(upper(ground_truth), upper(extracted), threshold);
        WHEN 'numeric' THEN
            RETURN numeric_similarity(ground_truth, extracted, tolerance);
        WHEN 'phase' THEN
            RETURN phase_similarity(ground_truth, extracted, threshold);
        ELSE
            -- Case-insensitive exact match fallback
            IF LOWER(COALESCE(ground_truth, '')) = LOWER(COALESCE(extracted, '')) THEN
                RETURN 1.0;
            ELSE
                RETURN 0.0;
            END IF;
    END CASE;
END;
$BODY$
  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER