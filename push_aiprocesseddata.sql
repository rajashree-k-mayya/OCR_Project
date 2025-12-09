CREATE OR REPLACE FUNCTION push_aiprocesseddata(in in_respid int4, in in_cpa int4, in inputjson text, in actid int4)
  RETURNS void AS 
$BODY$
DECLARE result_json jsonb; existing_json jsonb;gt_cpas int[];
BEGIN
    INSERT INTO assignmentpreelogs(fntext, created) SELECT 'push_aiprocesseddata ('||$1||','||$2||','||$3||','||$4||')',now();

    if $4=72 then
        gt_cpas := ARRAY[206, 113, 114, 855, 910, 1404];
    elsif $4=73 then
        gt_cpas := ARRAY[677, 1105, 1106, 1416, 742, 740];
    else 
        return;
    end if;
    

        WITH ground_truth_data AS (
            SELECT in_respid as responselogid, items."cpaRid" AS cpa, items."value"
            FROM jsonb_to_recordset($3::jsonb) AS items("cpaRid" INT, "value" TEXT, "valueRid" INT)
            WHERE  items."cpaRid" = ANY(gt_cpas)
            and $3 IS NOT NULL AND $3::text NOT IN ('{}','[]')
            /*
            union all
            
            SELECT rl.responselogid, items.categorypropertyallocationid AS cpa, items.value
            FROM tblresponselogs rl
            LEFT JOIN jsonb_to_recordset(rl.response::jsonb -> 'propertiesBean') AS items(value TEXT, categorypropertyallocationid INT)ON TRUE
            WHERE rl.responselogid = $1 AND items.categorypropertyallocationid = ANY(gt_cpas) and ($3 IS NULL OR $3::text IN ('{}','[]'))

            */
        ),
        /*
        else
        -- Step 1: Ground Truth Data
            WITH ground_truth_data AS (
                SELECT rl.responselogid, items.categorypropertyallocationid AS cpa, items.value
                FROM tblresponselogs rl
                LEFT JOIN jsonb_to_recordset(rl.response::jsonb -> 'propertiesBean') AS items(value TEXT, categorypropertyallocationid INT)ON TRUE
                WHERE rl.responselogid = $1 AND items.categorypropertyallocationid IN (206, 113, 114, 855, 910, 1404)
            ),
            */
        -- Step 2: Extracted AI Data
            extracted_data AS (
                SELECT r.responselogid,
                    -- old meter number
                    MAX(subprop->>'value') FILTER (
                        WHERE (subprop->>'categorypropertyallocationid')::int = 
                              CASE WHEN $4 = 72 THEN 1389 ELSE 1421 END
                    ) AS ocr_oldmeter_number,

                    -- old meter reading
                    MAX(subprop->>'value') FILTER (
                        WHERE (subprop->>'categorypropertyallocationid')::int = 
                              CASE WHEN $4 = 72 THEN 1390 ELSE 1414 END
                    ) AS ocr_oldmeter_reading,

                    -- old meter kvarh reading
                    MAX(subprop->>'value') FILTER (
                        WHERE (subprop->>'categorypropertyallocationid')::int = 
                              CASE WHEN $4 = 72 THEN 1401 ELSE 1418 END
                    ) AS ocr_oldmeter_kvarh_reading,

                    -- old meter exportkwh reading
                    MAX(subprop->>'value') FILTER (
                        WHERE (subprop->>'categorypropertyallocationid')::int = 
                              CASE WHEN $4 = 72 THEN 1406 ELSE 1411 END
                    ) AS ocr_oldmeter_exportkwh_reading,

                    -- old meter importkwh reading
                    MAX(subprop->>'value') FILTER (
                        WHERE (subprop->>'categorypropertyallocationid')::int = 
                              CASE WHEN $4 = 72 THEN 1403 ELSE 1408 END
                    ) AS ocr_oldmeter_importkwh_reading,

                    -- old meter number_sm
                    MAX(subprop->>'value') FILTER (
                        WHERE (subprop->>'categorypropertyallocationid')::int = 
                              CASE WHEN $4 = 72 THEN 1402 ELSE 1407 END
                    ) AS ocr_oldmeter_number_sm

                FROM tblresponselogs r
                CROSS JOIN LATERAL jsonb_array_elements(r.response::jsonb -> 'propertiesBean') AS prop
                CROSS JOIN LATERAL jsonb_array_elements(prop->'subPropertyList') AS subprop
                WHERE r.responselogid = $1 
                  AND (
                        ($4 = 72 AND (prop->>'categorypropertyallocationid')::int IN (45, 46, 969, 970))
                     OR ($4 = 73 AND (prop->>'categorypropertyallocationid')::int IN (739, 741, 1107, 1108))
                  )
                GROUP BY r.responselogid
            )
            ,

        -- Step 3: Combine and Compare
            combined_result AS (
                SELECT 
                    gt.responselogid, 
                    gt.cpa, 
                    gt.value AS ground_val,
                    CASE 
                        WHEN $4 = 72 AND gt.cpa = 206 THEN ed.ocr_oldmeter_number
                        WHEN $4 = 72 AND gt.cpa = 113 THEN ed.ocr_oldmeter_reading
                        WHEN $4 = 72 AND gt.cpa = 114 THEN ed.ocr_oldmeter_kvarh_reading
                        WHEN $4 = 72 AND gt.cpa = 855 THEN ed.ocr_oldmeter_exportkwh_reading
                        WHEN $4 = 72 AND gt.cpa = 910 THEN ed.ocr_oldmeter_importkwh_reading
                        WHEN $4 = 72 AND gt.cpa = 1404 THEN ed.ocr_oldmeter_number_sm
                        WHEN $4 = 73 AND gt.cpa = 677 THEN ed.ocr_oldmeter_number_sm
                        WHEN $4 = 73 AND gt.cpa = 1105 THEN ed.ocr_oldmeter_importkwh_reading
                        WHEN $4 = 73 AND gt.cpa = 1106 THEN ed.ocr_oldmeter_exportkwh_reading
                        WHEN $4 = 73 AND gt.cpa = 1416 THEN ed.ocr_oldmeter_number
                        WHEN $4 = 73 AND gt.cpa = 740 THEN ed.ocr_oldmeter_reading
                        WHEN $4 = 73 AND gt.cpa = 742 THEN ed.ocr_oldmeter_kvarh_reading
                        
                    END AS ocr_val,
                    CASE 
                        WHEN $4 = 72 AND gt.cpa IN (206) THEN compare_ai_values(gt.value, ed.ocr_oldmeter_number, 'string')
                        WHEN $4 = 72 AND gt.cpa IN (1404) THEN compare_ai_values(gt.value, ed.ocr_oldmeter_number_sm, 'string')
                        WHEN $4 = 72 AND gt.cpa IN (113) THEN compare_ai_values(gt.value, ed.ocr_oldmeter_reading, 'numeric')
                        WHEN $4 = 72 AND gt.cpa IN (114) THEN compare_ai_values(gt.value, ed.ocr_oldmeter_kvarh_reading, 'numeric')
                        WHEN $4 = 72 AND gt.cpa IN (855) THEN compare_ai_values(gt.value, ed.ocr_oldmeter_exportkwh_reading, 'numeric')
                        WHEN $4 = 72 AND gt.cpa IN (910) THEN compare_ai_values(gt.value, ed.ocr_oldmeter_importkwh_reading, 'numeric')
                        WHEN $4 = 73 AND gt.cpa IN (677) THEN compare_ai_values(gt.value, ed.ocr_oldmeter_number_sm, 'string')
                        WHEN $4 = 73 AND gt.cpa IN (742) THEN compare_ai_values(gt.value, ed.ocr_oldmeter_kvarh_reading, 'numeric')
                        WHEN $4 = 73 AND gt.cpa IN (1105) THEN compare_ai_values(gt.value, ed.ocr_oldmeter_importkwh_reading, 'numeric')
                        WHEN $4 = 73 AND gt.cpa IN (1106) THEN compare_ai_values(gt.value, ed.ocr_oldmeter_exportkwh_reading, 'numeric')
                        WHEN $4 = 73 AND gt.cpa IN (1416) THEN compare_ai_values(gt.value, ed.ocr_oldmeter_number, 'string')
                        WHEN $4 = 73 AND gt.cpa IN (740) THEN compare_ai_values(gt.value, ed.ocr_oldmeter_reading, 'numeric')
                        
 END AS accuracy
                FROM ground_truth_data gt
                INNER JOIN extracted_data ed 
                    ON gt.responselogid = ed.responselogid
                WHERE 
                    ($4 = 72 AND gt.cpa IN (206, 113, 114, 855, 910, 1404)) OR
                    ($4 = 73 AND gt.cpa IN (1416, 740, 742, 677, 1105, 1106))
                    AND gt.value IS NOT NULL
            )

        -- Step 4: Build JSON
        SELECT jsonb_agg(
            jsonb_build_object(
                'cpa', cpa,
                'accuracy', accuracy,
                'ground_val', ground_val,
                'ocr_val', ocr_val
            )
        ) INTO result_json
        FROM combined_result;

        -- Step 5: Exit if No Data
        IF result_json IS NULL THEN
            RAISE NOTICE 'No AI processed data found for responselogid = %', $1;
            RETURN;
        END IF;

        -- Step 6: Check existing data
        SELECT response_json INTO existing_json
        FROM tbl_aiimage_processeddata  WHERE responselogid = $1;

        IF existing_json IS NULL THEN
            -- Insert
            INSERT INTO tbl_aiimage_processeddata (responselogid, response_json)
            VALUES ($1, result_json);
        ELSE
            -- Merge and Update
            SELECT jsonb_agg(elem) INTO result_json
            FROM (
                SELECT DISTINCT ON ((elem->>'cpa')::int) elem
                FROM (
                    SELECT jsonb_array_elements(result_json) AS elem
                    UNION ALL
                    SELECT jsonb_array_elements(existing_json) AS elem
                ) all_elems
                ORDER BY (elem->>'cpa')::int
            ) merged;

            UPDATE tbl_aiimage_processeddata SET response_json = result_json, triggerdatetime = NOW()
            WHERE responselogid = $1;
        END IF;


END;
$BODY$
  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER