CREATE OR REPLACE FUNCTION gethighlighter(in categorypropertyallocationid int4, in userid int4, in projectid int4, in olid int4, in activityid int4, in inputjson text, in activitytype int4, in resplogid int4)
  RETURNS TABLE (msg text) AS 
$BODY$
declare ocrdata record;ocr_actualdata record; fill_color record;border_color record;textvar TEXT; 
result_json jsonb;
BEGIN
    insert into assignmentpreelogs(fntext,created) select 'gethighlighter'||$1::text||'/'||$2::text||'/'||$3::text||'/'||$4::text||'/'||$5::text||'/'||$6||'/'||$7::text||'/'||$8::text,now();


/*
if $3=13 and $5=72 then 
BEGIN
        --OCR data implementation
        SELECT
        MAX(sub_value) FILTER (WHERE sub_cpa = 1389) AS "1389",
        MAX(sub_value) FILTER (WHERE sub_cpa = 1390) AS "1390" into ocrdata
        FROM (
            SELECT 
                (subprop->>'categorypropertyallocationid')::int AS sub_cpa,
                subprop->>'value' AS sub_value
            FROM tblresponselogs r,
                 LATERAL jsonb_array_elements(r.response::jsonb->'propertiesBean') AS prop,
                 LATERAL jsonb_array_elements(prop->'subPropertyList') AS subprop
            WHERE r.responselogid = $8
              AND (prop->>'categorypropertyallocationid')::int IN (45)
        ) AS subquery;

        SELECT 
        MAX(value) FILTER (WHERE items.categorypropertyallocationid = 113) AS "113",
        MAX(value) FILTER (WHERE items.categorypropertyallocationid = 206) AS "206" into ocr_actualdata
        FROM tblresponselogs r
        CROSS JOIN LATERAL jsonb_to_recordset(r.response::jsonb -> 'propertiesBean') AS items(value text, categorypropertyallocationid int)
        WHERE r.responselogid = $8 AND items.categorypropertyallocationid IN (113,206);

    if (ocrdata."1390"=ocr_actualdata."113" and upper(ocrdata."1389")=upper(ocr_actualdata."206")) then 
    return query (select 
     '[{
                    "cpaRid": 113,
                    "setFocus": 0,
                    "categoryRid": 2,
                    "colorCode": {"fillColor": "#10b592"}
        },
        {
                    "cpaRid": 206,
                    "setFocus": 0,
                    "categoryRid": 2,
                    "colorCode": {"fillColor": "#10b592"}
         }]' ); 

    elsif (ocrdata."1390"=ocr_actualdata."113" and upper(ocrdata."1389")<>upper(ocr_actualdata."206")) then 
    return query (select 
     '[{
                    "cpaRid": 113,
                    "setFocus": 0,
                    "categoryRid": 2,
                    "colorCode": {"fillColor": "#10b592"}
        },
        {
                    "cpaRid": 206,
                    "setFocus": 0,
                    "categoryRid": 2,
                    "colorCode": {"fillColor": "#f1958a"}
         }]' ); 
    elsif (ocrdata."1390"<>ocr_actualdata."113" and upper(ocrdata."1389")=upper(ocr_actualdata."206")) then 
    return query (select 
     '[{
                    "cpaRid": 113,
                    "setFocus": 0,
                    "categoryRid": 2,
                    "colorCode": {"fillColor": "#f1958a"}
        },
        {
                    "cpaRid": 206,
                    "setFocus": 0,
                    "categoryRid": 2,
                    "colorCode": {"fillColor": "#10b592"}
         }]' ); 
    else return query (select 
     '[{
                    "cpaRid": 113,
                    "setFocus": 0,
                    "categoryRid": 2,
                    "colorCode": {"fillColor": "#f1958a"}
        },
        {
                    "cpaRid": 206,
                    "setFocus": 0,
                    "categoryRid": 2,
                    "colorCode": {"fillColor": "#f1958a"}
         }]' ); 

    end if;
END;
else return query(select '');
end if;


*/

    IF $3 = 13 AND $5=72 THEN

    PERFORM push_aiprocesseddata ( $8 , 45 , $6, $5);

    INSERT INTO assignmentpreelogs(fntext, created) SELECT 'gethighlighter called for userid = ' || $2, NOW();

        SELECT 
            MAX(CASE WHEN cpa = 206 THEN CASE WHEN accuracy * 100 = 100 and uppeR(ground_val) = upper(ocr_val) THEN '#7dcea0' WHEN accuracy * 100 >= 80.0 and accuracy * 100 < 100 THEN '#f0b27a' ELSE '#f1948a' END END) _206,
            MAX(CASE WHEN cpa = 113 THEN CASE WHEN accuracy * 100 = 100 THEN '#7dcea0' WHEN accuracy * 100 >= 80.0 and accuracy * 100 < 100 THEN '#f0b27a' ELSE '#f1948a' END END) _113,
            MAX(CASE WHEN cpa = 114 THEN CASE WHEN accuracy * 100 = 100 THEN '#7dcea0' WHEN accuracy * 100 >= 80.0 and accuracy * 100 < 100 THEN '#f0b27a' ELSE '#f1948a' END END) _114,
            MAX(CASE WHEN cpa = 855 THEN CASE WHEN accuracy * 100 = 100 THEN '#7dcea0' WHEN accuracy * 100 >= 80.0 and accuracy * 100 < 100 THEN '#f0b27a' ELSE '#f1948a' END END) _855,
            MAX(CASE WHEN cpa = 910 THEN CASE WHEN accuracy * 100 = 100 THEN '#7dcea0' WHEN accuracy * 100 >= 80.0 and accuracy * 100 < 100 THEN '#f0b27a' ELSE '#f1948a' END END) _910,
            MAX(CASE WHEN cpa = 1404 THEN CASE WHEN accuracy * 100 = 100 THEN '#7dcea0' WHEN accuracy * 100 >= 80.0 and accuracy * 100 < 100 THEN '#f0b27a' ELSE '#f1948a' END END) _1404 

        INTO fill_color
        FROM tbl_aiimage_processeddata, jsonb_to_recordset(response_json) AS item( cpa INT, ground_val TEXT, ocr_val TEXT, accuracy FLOAT)
        WHERE responselogid = $8;

        SELECT 
            MAX(CASE WHEN cpa = 206 THEN CASE WHEN accuracy * 100 = 100 and uppeR(ground_val) = upper(ocr_val) THEN '#27ae60' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f39c12' ELSE '#e74c3c' END END) _206,
            MAX(CASE WHEN cpa = 113 THEN CASE WHEN accuracy * 100 = 100 THEN '#27ae60' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f39c12' ELSE '#e74c3c' END END) _113,
            MAX(CASE WHEN cpa = 114 THEN CASE WHEN accuracy * 100 = 100 THEN '#27ae60' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f39c12' ELSE '#e74c3c' END END) _114, 
            MAX(CASE WHEN cpa = 855 THEN CASE WHEN accuracy * 100 = 100 THEN '#27ae60' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f39c12' ELSE '#e74c3c' END END) _855, 
            MAX(CASE WHEN cpa = 910 THEN CASE WHEN accuracy * 100 = 100 THEN '#27ae60' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f39c12' ELSE '#e74c3c' END END) _910, 
            MAX(CASE WHEN cpa = 1404 THEN CASE WHEN accuracy * 100 = 100 THEN '#27ae60' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f39c12' ELSE '#e74c3c' END END) _1404 
 
        INTO border_color
        FROM tbl_aiimage_processeddata,jsonb_to_recordset(response_json) AS item( cpa INT, ground_val TEXT, ocr_val TEXT, accuracy FLOAT)
        WHERE responselogid = $8;

        -- Construct JSON with individual colors
        SELECT jsonb_agg(
               jsonb_build_object(
                   'cpaRid', cpa,
                   'setFocus', 0,
                   'categoryRid', 2,
                   'colorCode', jsonb_build_object(
                       'labelColor', '#000000',
                       'valueColor', '#000000',
                       'borderColor', COALESCE(b_color, '#e74c3c'),
                       'fillColor',  COALESCE(f_color,  '#f1948a')
                   )
               )
           )
        INTO result_json
        FROM (
            SELECT 206 AS cpa, fill_color._206 AS f_color, border_color._206 AS b_color
            UNION ALL
            SELECT 113, fill_color._113, border_color._113
            UNION ALL
            SELECT 114, fill_color._114, border_color._114
            UNION ALL
            SELECT 855, fill_color._855, border_color._855
            UNION ALL
            SELECT 910, fill_color._910, border_color._910
            UNION ALL
            SELECT 1404, fill_color._1404, border_color._1404
        ) t;

        RETURN QUERY SELECT result_json::text;


    elsif $3=13 and $5=73 then



        PERFORM push_aiprocesseddata ( $8 , 739, $6 , $5);

        INSERT INTO assignmentpreelogs(fntext, created) SELECT 'gethighlighter called for userid = ' || $2, NOW();

            SELECT 
                MAX(CASE WHEN cpa = 1416 THEN CASE WHEN accuracy * 100 = 100 THEN '#7dcea0' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f0b27a' ELSE '#f1948a' END END) _1416,
                MAX(CASE WHEN cpa = 740 THEN CASE WHEN accuracy * 100 = 100 THEN '#7dcea0' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f0b27a' ELSE '#f1948a' END END) _740,
                MAX(CASE WHEN cpa = 742 THEN CASE WHEN accuracy * 100 = 100 THEN '#7dcea0' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f0b27a' ELSE '#f1948a' END END) _742,
                MAX(CASE WHEN cpa = 677 THEN CASE WHEN accuracy * 100 = 100 THEN '#7dcea0' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f0b27a' ELSE '#f1948a' END END) _677,
                MAX(CASE WHEN cpa = 1105 THEN CASE WHEN accuracy * 100 = 100 THEN '#7dcea0' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f0b27a' ELSE '#f1948a' END END) _1105,
                MAX(CASE WHEN cpa = 1106 THEN CASE WHEN accuracy * 100 = 100 THEN '#7dcea0' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f0b27a' ELSE '#f1948a' END END) _1106 

            INTO fill_color
            FROM tbl_aiimage_processeddata, jsonb_to_recordset(response_json) AS item( cpa INT, ground_val TEXT, ocr_val TEXT, accuracy FLOAT)
            WHERE responselogid = $8;

            SELECT 
                MAX(CASE WHEN cpa = 1416 THEN CASE WHEN accuracy * 100 = 100 THEN '#27ae60' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f39c12' ELSE '#e74c3c' END END) _1416,
                MAX(CASE WHEN cpa = 740 THEN CASE WHEN accuracy * 100 = 100 THEN '#27ae60' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f39c12' ELSE '#e74c3c' END END) _740,
                MAX(CASE WHEN cpa = 742 THEN CASE WHEN accuracy * 100 = 100 THEN '#27ae60' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f39c12' ELSE '#e74c3c' END END) _742, 
                MAX(CASE WHEN cpa = 677 THEN CASE WHEN accuracy * 100 = 100 THEN '#27ae60' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f39c12' ELSE '#e74c3c' END END) _677, 
                MAX(CASE WHEN cpa = 1105 THEN CASE WHEN accuracy * 100 = 100 THEN '#27ae60' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f39c12' ELSE '#e74c3c' END END) _1105, 
                MAX(CASE WHEN cpa = 1106 THEN CASE WHEN accuracy * 100 = 100 THEN '#27ae60' WHEN accuracy * 100 >= 70.0 and accuracy * 100 < 100 THEN '#f39c12' ELSE '#e74c3c' END END) _1106 

            INTO border_color
            FROM tbl_aiimage_processeddata,jsonb_to_recordset(response_json) AS item( cpa INT, ground_val TEXT, ocr_val TEXT, accuracy FLOAT)
            WHERE responselogid = $8;

            -- Construct JSON with individual colors
            SELECT jsonb_agg(
                   jsonb_build_object(
                       'cpaRid', cpa,
                       'setFocus', 0,
                       'categoryRid', 84,
                       'colorCode', jsonb_build_object(
                           'labelColor', '#000000',
                           'valueColor', '#000000',
                           'borderColor', COALESCE(b_color, '#e74c3c'),
                           'fillColor',  COALESCE(f_color,  '#f1948a')
                       )
                   )
               )
            INTO result_json
            FROM (
                SELECT 1416 AS cpa, fill_color._1416 AS f_color, border_color._1416 AS b_color
                UNION ALL
                SELECT 740, fill_color._740, border_color._740
                UNION ALL
                SELECT 742, fill_color._742, border_color._742
                UNION ALL
                SELECT 677, fill_color._677, border_color._677
                UNION ALL
                SELECT 1105, fill_color._1105, border_color._1105
                UNION ALL
                SELECT 1106, fill_color._1106, border_color._1106
            ) t;

            RETURN QUERY SELECT result_json::text;

end if;

END;



$BODY$
  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER