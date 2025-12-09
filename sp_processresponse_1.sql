CREATE OR REPLACE FUNCTION sp_processresponse_1(in projid int4, in distnodetypeid int4, in actid int4, in resplogid int4)
  RETURNS void AS 
$BODY$
declare distrid int4;signalstr record;cur_reading numeric;consumption record;respstatusid int4;roleidval int4;valu record;valu1 int4;unqid varchar;valuse record;seuser text;resurveyid int4;respresurvey int4;
amidtemp int4;amid int4;str text;logstr record;mibillinfo record;kwhmi numeric;midate varchar;finalvar numeric; var_valcalc numeric;nextapproleid int4;kwhci numeric
;cibillinfo record;cidate varchar;holdroleid int4;scanbarstckrhlder text;tdcpdc varchar;metertype int4;oldmetrinstalledbasement int4;installedinmetallic int4;meter varchar
;maxsdid int4;resid int4;accble int4;pdccheck text;minewbillinfo record;kwhminew numeric;midatenew varchar;C_denied_A int4;indtcode varchar;indtname varchar;dnamedt record;stype varchar;ipdoc record;lotidi int4;plussurvey record;lotidnew int4;oldmetstatcode varchar;reason text;nextlab int4; lab_tesing_val int4;
pingvalu varchar;nextl2 int4; nextrole int4;submit int4;
ocrdata record;ocr_actualdata record;v_flag bool;kwhdiff int4;billtype varchar;ping_cpa_todlt record;importkwh int4;
begin
    insert into assignmentpreelogs(fntext,created) select 'sp_processresponse_1'||$1||'/'||$2||'/'||$3||'/'||$4,now();
	Select distnid into distrid from tblresponselogs where responselogid=$4 ;
    Select responsestatusid into respstatusid from tblresponselogs where responselogid=$4 ;
	Select uniqueid into unqid from tblresponselogs where responselogid=$4 ;
    Select nextapproverroleid into nextapproleid from tblresponselogs where responselogid=$4 ;
    Select resurvey into resurveyid from tblresponselogs where responselogid=$4 ;
    select max(responselogid) into respresurvey from tblresponselogs where projectid=$1 and activityid=$3 and responsestatusid=3 and resurvey=1 and distnid=distrid;
    Select surveytype into stype from tblresponselogs where responselogid=$4 ;


    if($3=3 and stype='New') then
       begin
          select upper(distributionnodename) dstname,upper(distributionnodecode)dstcode from tbldistributionnodes where distributionnodeid=distrid into plussurvey;
          update  tbldistributionnodes set distributionnodename=plussurvey.dstname,distributionnodecode=plussurvey.dstcode where distributionnodeid=distrid;
          update tblresponselogs set distname=plussurvey.dstname,distcode=plussurvey.dstcode where responselogid=$4;
       end;
    end if;

    if resurveyid in(1) and respstatusid=3 then PERFORM push_dist_udf_master(respresurvey,$1); end if;

    if exists (SELECT 1 FROM tblresponselogs WHERE responselogid = $4 AND activityid NOT IN (0, -1) AND ((surveytype = 'New') OR (surveytype = 'Existing' AND resurvey = 1 and responsestatusid=3)) or $3=3) then
        BEGIN
            if $3=3 or (respstatusid=3 and resurveyid=1) then update tbldistributionnodes set path=null where distributionnodeid=distrid; end if;
            
            WITH RECURSIVE cte AS (
            SELECT distributionnodeid, underdistributionnodeid, distributionnodeid::text AS path,1 lvl
            FROM tbldistributionnodes
            WHERE  (distributionnodeid in(select distnodeid from getparentnodes(distrid)) or distributionnodeid=distrid
                    or distributionnodeid in(select distributionnodeid from tbldistributionnodes where path is null and distributionnodetypeid is not null)
                    AND distributionnodetypeid is not null)
            UNION ALL
            SELECT t.distributionnodeid, t.underdistributionnodeid, (p.path || '.' || t.distributionnodeid::text) AS path,lvl+1
            FROM tbldistributionnodes AS t
            INNER JOIN cte AS p ON t.underdistributionnodeid = p.distributionnodeid
            where t.distributionnodetypeid is not null   ) 
            ,cte1 as
            (select distributionnodeid,path 
            from(select distributionnodeid,path,row_number() over(partition by distributionnodeid order by lvl desc) rn from cte) as P 
            where rn=1)
            UPDATE tbldistributionnodes AS t
            SET path = cte1.path::ltree
            FROM cte1
            WHERE  t.path is null  and t.distributionnodeid = cte1.distributionnodeid;
        END;
    end if;

    if exists (select 1 from tblresponselogs where uniqueid=unqid and responselogid<>$4 and responsestatusid<>-1 and projectid=13) then
        BEGIN
            update tblresponselogs set projectid=999, responsestatusid=-1,nextapproverroleid=0,rejectreason='Duplicate Activity Performed - 1' where responselogid=$4;
            update tblsurveydetaails set distributionnodeid=1 where responselogid=$4;
        END;
    else
    BEGIN
        if $3=72 then
            BEGIN
        --       ---3 level bucket
        --        select roleid into holdroleid from tblprojectuserallocation where userid in(Select nextapproverid from tblresponselogs where responselogid=$4 ) and projectid=13;
        --
        --        if nextapproleid=9 or holdroleid=9 then
        --             BEGIN
        --
        --                select billread,to_date(billdate,'dd-MM-yyyy') billdate 
        --                from (
        --                select row_number() over(order by discombillreadid desc) rn,billread ,billdate
        --                from tbldiscombillreads where activityid=72 and distnid=distrid) as P
        --                where rn=1 into mibillinfo;
        --
        --                select value into kwhmi
        --                from tblresponselogs
        --                cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
        --                where responselogid=$4 and items.categorypropertyallocationid in (113);
        --
        --                select to_date(surveydate,'dd/MM/yyyy') into midate from tblresponselogs where responselogid=$4;
        --
        --                update tblresponselogs set responsestatusid=(case when mibillinfo.billread is null then 24 when (kwhmi-mibillinfo.billread)<0 then 22 
        --                                                                  when (kwhmi-mibillinfo.billread)>1000 then 23 when (kwhmi-mibillinfo.billread) between 0 and 1000  then 25 else 5 end) 
        --                where responselogid=$4;
        --
        --            END;
        --       end if;


        --        --sm 50
        --        if not exists(select 1 from tblsm_50 where distributionnodeid=distrid and valid=true) and nextapproleid=9 then
        --               perform  fn_update_sm50status(distrid,$4);
        --        end if;
        --
        --         --lot
        --        select  distnid,isresurvey,serveyorid,locationid,surveydate,a.lotid,nextapproverroleid,appoveddate  from tblresponselogs  a where responselogid=$4 into ipdoc;
        --
        --        if ipdoc.nextapproverroleid in(68) then 
        --        BEGIN
        --           select * into lotidi from getlotid(ipdoc.locationid::int4,ipdoc.nextapproverroleid,to_date(ipdoc.appoveddate,'dd/MM/yyyy'),$3);
        --           update tblresponselogs set lotid=lotidi where responselogid=$4; 
        --         END;
        --        end if;
        --
        --
        --     --lotname delete
        --     IF EXISTS(select 1 from tbllots where lotid in(SELECT  lotid from tblresponselogs where responselogid=$4) and status=1) then 
        --       BEGIN
        --         SELECT  lotid into lotidnew from tblresponselogs where responselogid=$4;
        --           update tbllots set status=0 where lotid=lotidnew and status=1 and roleid=68 and lotid not in(select distinct lotid from tblresponselogs where lotid=lotidnew and responsestatusid in(6));
        --           update tbllots set status=0 where lotid=lotidnew and status=1 and roleid=10 and lotid not in(select distinct lotid from tblresponselogs where lotid=lotidnew and responsestatusid in(28));
        --
        --      END;
        --     end if;

        delete from etl_midata where projectid=$1 and activityid=$3 and distributionnodetypeid=$2 and distributionnodeid=distrid;

        --Ping test
        if resurveyid = 1 AND respstatusid = 3 THEN
            DELETE FROM tblresurveypropertyvaluedetails 
            WHERE resurveypropertyvaluedetailsid IN (
                SELECT DISTINCT d.resurveypropertyvaluedetailsid
                FROM tblresurvey a
                LEFT JOIN tblresurveydetaails b 
                       ON a.resurveyid = b.resurveyid
                INNER JOIN tblresurveyproperties c 
                       ON b.resurveydetailsid = c.resurveydetailsid
                INNER JOIN tblresurveypropertyvaluedetails d 
                       ON c.resurveypropertyid = d.resurveypropertyid
                WHERE a.responseid = $4
                  AND c.catogorypropertyallocationid IN (1385, 1386, 1387)
            );
        end if;
        ----ping test--
        
        --Consumer denied Access
        select "valueId" into C_denied_A
        from tblresponselogs
        cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items(value text, "valueId" int,categorypropertyallocationid int)
        where activityid=$3 and projectid=$1 and responselogid=$4 and items.categorypropertyallocationid in (772) and value='Yes' and response<>'';

        if C_denied_A=16529 then
           BEGIN
             update tblresponselogs set responsestatusid=27,nextapproverroleid=0,approved=1  where responselogid=$4 and activityid=$3;
             update tbldistributionnodes set lastactivityid=-1 where distributionnodeid=distrid and distributionnodetypeid=$2;
           END;
        end if;
        select roleid into holdroleid from tblprojectuserallocation where userid in(Select nextapproverid from tblresponselogs where responselogid=$4 ) and projectid=13;

        if nextapproleid in(9,68,10) and respstatusid<>2/*or holdroleid in(9,68,10)*/ then
           BEGIN
            --sm 50

--           if   (nextapproleid=9 or holdroleid=9) and pingvalu ilike '%fail%' then
--             update tblresponselogs  set responsestatusid = 54  where responselogid = $4;

        if $3=72 and (nextapproleid=9 or holdroleid=9) then--and not exists(select 1 from tblsm_50 where distributionnodeid=distrid and valid=true)  then
             --  perform  fn_update_sm50status(distrid,$4); 
       -- else
            --4 level bucket
           BEGIN    
            select billread,to_date(billdate,'dd-MM-yyyy') billdate 
            from (
            select row_number() over(order by discombillreadid desc) rn,billread ,billdate
            from tbldiscombillreads where activityid=72 and distnid=distrid) as P
            where rn=1 into mibillinfo;

            select value into kwhmi
            from tblresponselogs
            cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
            where responselogid=$4 and items.categorypropertyallocationid in (113);

            select to_date(surveydate,'dd/MM/yyyy') into midate from tblresponselogs where responselogid=$4;

            select value into oldmetstatcode
            from tblresponselogs
            cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
            where responselogid=$4 and items.categorypropertyallocationid in (53);

            select value into kwhdiff
            from tblresponselogs
            cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
            where responselogid=$4 and items.categorypropertyallocationid in (910);

            select value into billtype
            from tblresponselogs
            cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
            where responselogid=$4 and items.categorypropertyallocationid in (10);

            update tblresponselogs set responsestatusid=(case when oldmetstatcode='Faulty' then 29 when mibillinfo.billread is null  then 24 
                                                        when billtype not in ('Solar Meter') and (kwhmi-mibillinfo.billread)<0 then 22 
                                                        when billtype not in ('Solar Meter') and (kwhmi-mibillinfo.billread)>1000 then 23 
                                                        when billtype not in ('Solar Meter') and (kwhmi-mibillinfo.billread) between 0 and 1000  then 25 
                                                        when billtype in ('Solar Meter') and (kwhdiff-mibillinfo.billread)<0 then 22 
                                                        when billtype in ('Solar Meter') and (kwhdiff-mibillinfo.billread)>1000 then 23 
                                                        when billtype in ('Solar Meter') and (kwhdiff-mibillinfo.billread) between 0 and 1000  then 25 else 5 end) 
            where responselogid=$4;
          END;


        end if;
        --OCR auto aprroval
        if respstatusid in (25) and exists(select 1 from tbldiscombillreads where distnid=distrid) then
            PERFORM fn_ocr_autoapproval(distrid,$4);
        end if;

        --lot 
        select  distnid,isresurvey,serveyorid,locationid,surveydate,a.lotid,nextapproverroleid,appoveddate,a.responsestatusid  from tblresponselogs  a where responselogid=$4 into ipdoc;
 
        if ipdoc.nextapproverroleid in(68) and ipdoc.responsestatusid=25 then 
        BEGIN
            select * into lotidi from getlotid(ipdoc.locationid::int4,ipdoc.nextapproverroleid,to_date(ipdoc.appoveddate,'dd/MM/yyyy'),$3);
            update tblresponselogs set lotid=lotidi,rejected=0 where responselogid=$4; 
        END;
        end if; 
    END;
    end if;

   --billread tblbillreadshistory
    if nextapproleid in(10) or holdroleid in(10) then
    
        insert into tblbillreadshistory(distnid,responselogid,cpaid,oldvalue,newvalue,userid)
        select distnid,responselogid,items.categorypropertyallocationid,items."oldValue" ,value,approverid
        from tblresponselogs
        cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int,"oldValue" text)
        where responselogid=$4 and items.categorypropertyallocationid in (663);
    end if;


-- 	if $4>1069812 and respstatusid in(0,5) then 
--    BEGIN
--     select value::numeric into cur_reading from (
--     select  value 
--     from tblresponselogs
-- 	 cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
-- 	 where responselogid=$4 and items.categorypropertyallocationid in (113)) as P;
--
--     select b.avgconsumption::numeric ,b.prevreading::numeric into consumption
--     from tbldistributionnodes a
--     inner join tblapdclbill_avgdetails b on a.distributionnodecode=b.consumerno
--     where distributionnodeid=distrid;
--
--     if coalesce(consumption.avgconsumption,0)>0 then 
--     BEGIN
--        if cur_reading < coalesce(consumption.prevreading,0) then 
--        BEGIN
--           update tblresponselogs set responsestatusid=16 where responselogid=$4;
--           INSERT INTO iiplabnormalconsdet(responselogid)SELECT  $4 WHERE NOT EXISTS (SELECT responselogid FROM iiplabnormalconsdet WHERE responselogid = $4 );
--        END;
--        elseif  ((cur_reading - coalesce(consumption.prevreading,0))  > ((coalesce(consumption.avgconsumption,0)+(coalesce(consumption.avgconsumption,0)/2))))  or
--            (((cur_reading - coalesce(consumption.prevreading,0)) < (coalesce(consumption.avgconsumption,0)/2))) then 
--         BEGIN
--          update tblresponselogs set responsestatusid=16 where responselogid=$4;
--          INSERT INTO iiplabnormalconsdet(responselogid)SELECT  $4 WHERE NOT EXISTS (SELECT responselogid FROM iiplabnormalconsdet WHERE responselogid = $4 );
--        END;
--       end if;
--    end;
--    end if;
--
--     end;
-- end if;
--	

    
--OCR data implementation
    SELECT
    MAX(sub_value) FILTER (WHERE sub_cpa = 1389) AS "1389",
    MAX(sub_value) FILTER (WHERE sub_cpa = 1390) AS "1390",
    MAX(sub_value) FILTER (WHERE sub_cpa = 1401) AS "1401",
    MAX(sub_value) FILTER (WHERE sub_cpa = 1402) AS "1402",
    MAX(sub_value) FILTER (WHERE sub_cpa = 1403) AS "1403",
    MAX(sub_value) FILTER (WHERE sub_cpa = 1406) AS "1406" into ocrdata
    FROM (
        SELECT 
            (subprop->>'categorypropertyallocationid')::int AS sub_cpa,
            subprop->>'value' AS sub_value
        FROM tblresponselogs r,
             LATERAL jsonb_array_elements(r.response::jsonb->'propertiesBean') AS prop,
             LATERAL jsonb_array_elements(prop->'subPropertyList') AS subprop
        WHERE r.responselogid = $4
          AND (prop->>'categorypropertyallocationid')::int IN (45,46,969,970)
    ) AS subquery;

    SELECT 
    MAX(value) FILTER (WHERE categorypropertyallocationid = 113) AS "113",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 206) AS "206",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 114) AS "114",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 10) AS "billtype",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 25) AS "old_phase",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 1404) AS "1404",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 910) AS "910",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 855) AS "855" into ocr_actualdata
    FROM tblresponselogs r
    CROSS JOIN LATERAL jsonb_to_recordset(r.response::jsonb -> 'propertiesBean')
    AS items(value text, categorypropertyallocationid int)
    WHERE r.responselogid = $4
    AND items.categorypropertyallocationid IN (113,206,114,10,25,1404,910,855);

    if ocr_actualdata."old_phase" in ('LTCT') and (trim(ocrdata."1390")::numeric=trim(ocr_actualdata."113")::numeric and upper(trim(ocrdata."1389"))=upper(trim(ocr_actualdata."206"))
       and trim(ocrdata."1401")::numeric=trim(ocr_actualdata."114")::numeric ) then
            v_flag :=true;
    elsif ocr_actualdata."old_phase" in ('1-PH','3-PH') and (trim(ocrdata."1390")::numeric=trim(ocr_actualdata."113")::numeric and upper(trim(ocrdata."1389"))=upper(trim(ocr_actualdata."206"))) then
            v_flag :=true;
    elsif ocr_actualdata."billtype" in ('Solar Meter') and upper(trim(ocr_actualdata."1404"))=upper(trim(ocrdata."1402")) and trim(ocr_actualdata."910")::numeric=trim(ocrdata."1403")::numeric
          and trim(ocr_actualdata."855")::numeric=trim(ocrdata."1406")::numeric then 
            v_flag :=true;

    else 
        v_flag :=false;

    end if;

    if respstatusid in (0) and nextapproleid=11 then
        if  v_flag then
    --if respstatusid in (0) and nextapproleid=11 and (ocrdata."1390"=ocr_actualdata."113" and upper(ocrdata."1389")=upper(ocr_actualdata."206")
       -- and (ocr_actualdata."114" is null or ocrdata."1401"=ocr_actualdata."114")
    --)  then
        update tblresponselogs set responsestatusid=5, nextapproverroleid=9 where responselogid=$4;
        INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks) VALUES ($4, now(), 16, 5, '⭐ AI Verified : Normal Reading is auto-approved');
        insert into tbl_ocr_verified_data (responselogid, distributionnodeid, responsestatusid, nextapproverroleid)
        select responselogid,distnid,responsestatusid,nextapproverroleid from tblresponselogs where responselogid=$4;
        PERFORM  sp_processresponse_1($1,$2,$3,$4);
        return;
       else
        insert into tbl_ocr_nonverified_data (responselogid, distributionnodeid, responsestatusid, nextapproverroleid)
        select responselogid,distnid,responsestatusid,nextapproverroleid from tblresponselogs where responselogid=$4;

       end if;

    end if;

    if not exists(select 1 from tbl_aiimages_dataforreprocess where responselogid=$4 and categorypropertyallocationid in (45,46,969,970)) then
        PERFORM fn_aiimages_dataforreprocess($4);
    end if;


   if respstatusid in(1) then PERFORM push_dist_udf_master($4,$1); end if;
	
	
   WITH MinSD AS (
        SELECT * FROM (
        SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,SurveyDate,SurveyTime
                ,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID )) MaxNo
        FROM ( SELECT responselogid,projectid,activityid FROM tblresponselogs B WHERE b.ProjectID IN ($1)
                and b.nodetype in ($2) and B.activityid in($3)) r
        INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
        INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0 
            and d.distributionnodeid=distrid
        ) s WHERE MaxNo=1
        )
    , Sdata AS (
        SELECT s.ProjectID,s.ResponseLogID,s.DistributionNodeID,OLID,SurveyID,s.SurveyDate,s.SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
        ,DistributionNodeTypeID,DistributionNodeName,DistributionNodeCode,Approved,AppovedDate ApprovedDate,Rejected,RejectReason,ReSurveyDone,ResponseStatusID
        ,ApproverID,s.ActivityID,NextApproverRoleID,LocationID,nextapproverid,s1.SurveyDate Min_SurveyDate,s1.SurveyTime Min_SurveyTime,contactrid contactrid FROM (
    SELECT * FROM (
        SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,OLID,SurveyID,SurveyDate,SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
                ,s.ActivityID Survey_ActivityID
                ,distributionnodename,distributionnodecode,DistributionNodeTypeID,UnderDistributionNodeID 
                ,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID desc)) MaxNo
        FROM ( SELECT responselogid,projectid,responsedate,responsetime,approved,rejected,appoveddate,locationid,rejectreason,resurveydone,responsestatusid
                ,approverid,nextapproverid,activityid,nextapproverroleid,contact_rid contactrid	FROM tblresponselogs B WHERE b.ProjectID IN ($1)
                and b.nodetype in ($2)   and B.activityid in($3) and distnid=distrid ) r
        INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
        INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0
        ) s WHERE MaxNo=1 ) s
        LEFT JOIN MinSD s1 ON s1.DistributionNodeID=s.DistributionNodeID AND s1.activityid=s.activityid AND s1.projectid=s.projectid
        )
    ,crosstabcte as (
        select *  from crosstab (
        'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
        cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
        where responselogid='||$4||' and nodetype='|| $2 ||' and items.categorypropertyallocationid in (7,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,28,39,40,41,42,45,46,53,56,57,58,59,60,61,62
        ,63,64,65,67,68,69,70,71,72,73,74,75,76,77,81,86,97,98,99,102,109,110,111,112,27,200,201,202,5,80,206,175,204,207,208,213,212
        ,113,114,115,355,772,773,766,770,775,2,4,6,34,203,353,359,354,823,180,767,824,974,975,818,977,978,979,980,981,982,1042,1043,1013,1017,1014,1015,1016)',

        'select distinct ''_''|| categorypropertyallocationid 
        from tblcategorypropertyallocation q  
        right join tblproperties r on r.propertyid=q.propertyid where categoryid in(1,2,3,90)
        and categorypropertyallocationid in(7,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,28,39,40,41,42,45,46,53,56,57,58,59,60,61,62
        ,63,64,65,67,68,69,70,71,72,73,74,75,76,77,81,86,97,98,99,102,109,110,111,112,27,200,201,202,5,80,206,175,204,207,208,213,212
        ,113,114,115,355,772,773,766,770,775,2,4,6,34,203,353,359,354,823,180,767,824,974,975,818,977,978,979,980,981,982,1042,1043,1013,1017,1014,1015,1016)
            ORDER  BY ''_''|| categorypropertyallocationid '
         )
        as newtable (
            responselogid integer,_10 text,_1013 text,_1014 text,_1015 text,_1016 text,_1017 text,_102 text,_1042 text,_1043 text,_109 text,_11 text,_110 text,_111 text,_112 text,_113 text,_114 text,
            _115 text,_12 text,_13 text,_14 text,_15 text,_16 text,_17 text,_175 text
            ,_18 text,_180 text,_19 text,_2 text,_20 text,_200 text,_201 text
            ,_202 text,_203 text,_204 text,_206 text,_207 text,_208 text,_21 text,_212 text,_213 text,_22 text,_23 text,_24 text,_25 text
            ,_26 text,_27 text,_28 text,_34 text,_353 text,_354 text,_355 text,_359 text ,_39 text,_4 text,_40 text,_41 text,_42 text,_45 text,_46 text,_5 text,_53 text,_56 text
            ,_57 text,_58 text,_59 text,_6 text,_60 text,_61 text,_62 text
            ,_63 text,_64 text,_65 text,_67 text,_68 text,_69 text,_7 text,_70 text,_71 text,_72 text,_73 text,_74 text,_75 text,_76 text,_766 text,_767 text
            ,_77 text,_770 text,_772 text,_773 text,_775 text,_80 text,_81 text,_818 text,_823 text,_824 text,_86 text,_97 text,_974 text,_975 text,_977 text,_978 text,_979 text,_98 text,_980 text, _981 text,_982 text,_99 text

         )
        )insert into etl_midata
        select projectid, a.responselogid, distributionnodeid, olid, surveyid, surveydate, surveytime, surveytype, cycles, latitude, longitude, distributionnodetypeid, distributionnodename, distributionnodecode, approved, approveddate, rejected, rejectreason, resurveydone, responsestatusid, approverid, activityid, nextapproverroleid, locationid, nextapproverid, min_surveydate, min_surveytime 
        ,_10,_102,_109,_11,_110,_111,_112,_12,_13,_14,_15,_16,_17,_18,_19,_20,_21,_213,_22,_23,_24,_25,_26,_28,_39,_40,_41,_42,_45,_46,_53,_56,_57,_58,_59,_60,_61,_62,_63,_64,_65,_67
        ,_68,_69,_7,_70,_71,_72,_73,_74,_75,_76,_77,_81,_86,_97,_98,_99,_27,_200,_201,_202,_5,_80,_206,contactrid,_175,_204,_207,_208,_212
        ,_113,_114,_115,_355,_772,_773,_766,_770,_775,_2,_4,_6,_34,_203,_353,_359,_354,_823,_180,_767,_824,_974,_975,_818,_977,_978,_979,_980,_981,_982,_1042,_1043,_1013,_1014,_1015,_1016,_1017
        from sdata a
        left join crosstabcte b on a.responselogid=b.responselogid;

	update tblresponselogs set updated=false where responselogid=$4;
    insert into etl_pushresponselogFORetl(responselogid,activityid,etlupdated) select $4,$3,false ;
    INSERT INTO etl_pushresponselogforetl_dashboard (responselogid, activityid, etlupdated) select $4,$3,false ;
    insert into etl_pushslnoforetl (slno,etlupdated) select (case when coalesce(_57,'null') ='null' then _58 else _57::jsonb->>'meter_Id' end) ,false from etl_midata where responselogid=$4;
	--return 1;
	
	PERFORM push_dist_udf($4,$1);
    
    if respstatusid in(0) and nextapproleid in(11) then  
                insert into tp_meterinstall_searialno_push_logs (searialno,activityid,rl_rid,updated) 
                select (case when coalesce(_57,'null') ='null' then _58 else _57::jsonb->>'meter_Id' end) ,$3,$4,false from etl_midata a
                where a.responselogid=$4  and   a._11 not in('PDC','TDC');
    end if;

    with timectea as(
    select max(tblapprovallogid) logid from tblapprovallogs 
    where responselogid=$4
    )
    ,detailctea as(
    select tblapprovallogid,logtime,userid from tblapprovallogs
    )
    select to_char(logtime,'YYYY-MM-DD HH:MI:SS')logtime1,userid from timectea a
    left join detailctea b on b.tblapprovallogid=a.logid into logstr;

     update tblresponselogs set sla_details=jsonb_build_object('updatedTimeStamp',logstr.logtime1,'updatedByUserRid',logstr.userid)
     where responselogid=$4;

--     update tbllots set status=0 where status=1 and roleid=68 and lotid not in(select distinct lotid from tblresponselogs where responsestatusid in(6));
--     update tbllots set status=0 where status=1 and roleid=10 and lotid not in(select distinct lotid from tblresponselogs where responsestatusid in(28));

--package 2

 if not exists (select 1 from tbl_distinid_package where distinid=distrid) then 
        INSERT INTO tbl_distinid_package (distinid, activityid, activity_package, updateddatetime)  
        select a.distributionnodeid,a.activityid,b.warehouse_package,now() from  etl_midata a join tbl_meterno_package b
        on upper(a._57::jsonb->>'meter_Id')=b.serialno where a.responselogid=$4;
 end if;

END;

elsif $3=71 then

	BEGIN
       select  distnid,isresurvey,serveyorid,locationid,surveydate,a.lotid,nextapproverroleid,appoveddate  from tblresponselogs  a where responselogid=$4 into ipdoc;
        if ipdoc.nextapproverroleid in(68) then 
            BEGIN
                select * into lotidi from getlotid(ipdoc.locationid::int4,ipdoc.nextapproverroleid,to_date(ipdoc.appoveddate,'dd/MM/yyyy'),$3);
                update tblresponselogs set lotid=lotidi,rejected=0 where responselogid=$4; 
            END;
        end if;

--         IF EXISTS(select 1 from tbllots where lotid in(SELECT  lotid from tblresponselogs where responselogid=$4) and status=1) then 
--         BEGIN
--             SELECT  lotid into lotidnew from tblresponselogs where responselogid=$4;
--             update tbllots set status=0 where lotid=lotidnew and status=1 and roleid=68 and lotid not in(select distinct lotid from tblresponselogs where lotid=lotidnew and responsestatusid in(6));
--             update tbllots set status=0 where lotid=lotidnew and status=1 and roleid=10 and lotid not in(select distinct lotid from tblresponselogs where lotid=lotidnew and responsestatusid in(28));
--        END;
--        end if;

     delete from etl_cidata where projectid=$1 and activityid=$3 and distributionnodetypeid=$2 and distributionnodeid=distrid;

     --Consumer denied Access
     select "valueId" into C_denied_A
     from tblresponselogs
     cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items(value text, "valueId" int,categorypropertyallocationid int)
     where activityid=$3 and projectid=$1 and responselogid=$4 and items.categorypropertyallocationid in (772) and value='Yes' and response<>'';

     if C_denied_A=16529 then
        BEGIN
             update tblresponselogs set responsestatusid=27,nextapproverroleid=0,approved=1  where responselogid=$4 and activityid=$3;
             update tbldistributionnodes set lastactivityid=-1 where distributionnodeid=distrid and distributionnodetypeid=$2;
        END;
     end if;

    select value into tdcpdc
    from tblresponselogs
    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items(value text,categorypropertyallocationid int)
    where responselogid=$4 and activityid=$3 and projectid=$1 and responselogid=$4 and items.categorypropertyallocationid in (11) and response<>'';

    if tdcpdc in ('PDC','TDC') and respstatusid=1 then
        BEGIN
            if exists(select responselogid from tblresponselogs where projectid=$1 and activityid=$3 and responselogid=$4 and nextapproverroleid=0 and responsestatusid=1) then
            BEGIN
                --update tblresponselogs set responsestatusid=27,nextapproverroleid=0,approved=1  where responselogid=$4 and activityid=$3;
                update tbldistributionnodes set lastactivityid=-1 where distributionnodeid=distrid and distributionnodetypeid=$2;
            END;
            end if;
        END;
     end if;
	
    if  $4>1069812 and respstatusid in(0,5) then 
        BEGIN
            select val::jsonb->>'network_provider' netprovider1,val::jsonb->>'signal_strength' signalstr1 
            ,val::jsonb->>'network_provider_2' netprovider2,val::jsonb->>'signal_strength_2'  signalstr2  into signalstr
            from (
            select responselogid, value::text val from tblresponselogs
            cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
            where responselogid=$4 and items.categorypropertyallocationid in (34))as P;
 
            if (( upper(signalstr.netprovider1) like '%JIO%' and signalstr.signalstr1::int4<-150)  or (upper(signalstr.netprovider2) like '%JIO%' and signalstr.signalstr2::int4<-150))
               then update tblresponselogs set responsestatusid=15 where responselogid=$4;
            end if;
        END;
    end if;
   
    select roleid into holdroleid from tblprojectuserallocation where userid in(Select nextapproverid from tblresponselogs where responselogid=$4 ) and projectid=13;

    if nextapproleid in (9,78) --or holdroleid=9 
    then
        BEGIN
    
        select billread,to_date(billdate,'dd-MM-yyyy') billdate 
        from (
        select row_number() over(order by discombillreadid desc) rn,billread ,billdate
        from tbldiscombillreads where activityid=71 and distnid=distrid) as P
        where rn=1 into cibillinfo;

        select value into kwhci
        from tblresponselogs
        cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
        where responselogid=$4 and items.categorypropertyallocationid in (39);

        select to_date(surveydate,'dd/MM/yyyy') into cidate from tblresponselogs where responselogid=$4;

        with calc_cte as
        (select (case when (extract(day from cidate::timestamp - cibillinfo.billdate::timestamp))=0 then 
                (case when (kwhci-cibillinfo.billread)<0 then -1 when (kwhci-cibillinfo.billread)>=500 then 31 when (kwhci-cibillinfo.billread)<500 then 29 else 25 end) 
                 else (kwhci-cibillinfo.billread)/(extract(day from cidate::timestamp - cibillinfo.billdate::timestamp)) end ) valcalc)
        select valcalc into var_valcalc from calc_cte;

--         update tblresponselogs set responsestatusid=(case when var_valcalc is null then 24 when var_valcalc<0 then 22
--                                                           when var_valcalc>30 then 23 when var_valcalc>=0 and var_valcalc<=30 then 25 else 5 end) where responselogid=$4;
--        
--    
        END;
    end if;

    if respstatusid in(1) then PERFORM push_dist_udf_master($4,$1); end if;
	
	WITH MinSD AS (
	SELECT * FROM (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,SurveyDate,SurveyTime
			,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID )) MaxNo
	FROM ( SELECT responselogid,projectid,activityid FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2) and B.activityid in($3)) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0 
		and d.distributionnodeid=distrid
	) s WHERE MaxNo=1
	)
	, Sdata AS (
	SELECT s.ProjectID,s.ResponseLogID,s.DistributionNodeID,OLID,SurveyID,s.SurveyDate,s.SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
	,DistributionNodeTypeID,DistributionNodeName,DistributionNodeCode,Approved,AppovedDate ApprovedDate,Rejected,RejectReason,ReSurveyDone,ResponseStatusID
	,ApproverID,s.ActivityID,NextApproverRoleID,LocationID,nextapproverid,s1.SurveyDate Min_SurveyDate,s1.SurveyTime Min_SurveyTime,contact_rid contactrid FROM (
	SELECT * FROM (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,OLID,SurveyID,SurveyDate,SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
			,s.ActivityID Survey_ActivityID
			,distributionnodename,distributionnodecode,DistributionNodeTypeID,UnderDistributionNodeID
			,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID desc)) MaxNo
	FROM ( SELECT responselogid,projectid,responsedate,responsetime,approved,rejected,appoveddate,locationid,rejectreason,resurveydone,responsestatusid
			,approverid,nextapproverid,activityid,nextapproverroleid,contact_rid	FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2)   and B.activityid in($3) and distnid=distrid ) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0
	) s WHERE MaxNo=1 ) s
	LEFT JOIN MinSD s1 ON s1.DistributionNodeID=s.DistributionNodeID AND s1.activityid=s.activityid AND s1.projectid=s.projectid
	)
	,crosstabcte as (
	select *  from crosstab (
	'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
	cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
	where responselogid='||$4||' and nodetype='|| $2 ||' and items.categorypropertyallocationid in (1,10,100,101,102,109,11,110,111,112,113,114,115,12,13,14,15,16,17,175,176,18,180,181,19,2,20,205,206,21,210,212,213,22,23,24,25,26,27,28,29,3,30,31,317,32,33,34,35,352,355,357,36,37,372,38,382,39,4,40,41,42,43,44,47,49,5,50,51,52,53,54,55,6,663,664,666,667,669,7,76,766,767,77,770,772,773,774,775,8,80,
          81,82,83,84,85,86,87,88,89,9,90,91,93,94,95,98,99,823,824,910,855,968)',

	'select distinct ''_''|| categorypropertyallocationid 
	from tblcategorypropertyallocation q  
	right join tblproperties r on r.propertyid=q.propertyid where categoryid in(1,2,6,7,8)
	and categorypropertyallocationid in(1,10,100,101,102,109,11,110,111,112,113,114,115,12,13,14,15,16,17,175,176,18,180,181,19,2,20,205,206,21,210,212,213,22,23,24,25,26,27,28,29,3,30,31,317,32,33,34,35,352,355,357,36,37,372,38,382,39,4,40,41,42,43,44,47,49,5,50,51,52,53,54,55,6,663,664,666,667,669,7,76,766,767,77,770,
          772,773,774,775,8,80,81,82,83,84,85,86,87,88,89,9,90,91,93,94,95,98,99,823,824,910,855,968)
		ORDER  BY ''_''|| categorypropertyallocationid '
	 )
	 as newtable (
	 responselogid integer,_1 text ,_10 text ,_100 text ,_101 text ,_102 text ,_109 text ,_11 text ,_110 text ,_111 text ,_112 text ,_113 text ,_114 text ,_115 text ,_12 text ,_13 text ,_14 text ,_15 text ,_16 text ,_17 text ,_175 text ,_176 text ,_18 text ,_180 text ,_181 text ,_19 text ,_2 text ,_20 text ,_205 text ,_206 text ,_21 text ,_210 text ,_212 text ,_213 text ,_22 text ,_23 text ,_24 text ,_25 text ,_26 text ,_27 text ,_28 text ,_29 text ,_3 text ,_30 text ,_31 text ,_317 text ,_32 text ,_33 text ,_34 text ,_35 text ,_352 text ,_355 text ,_357 text ,_36 text ,_37 text ,_372 text ,_38 text ,_382 text ,_39 text ,_4 text ,_40 text ,_41 text ,_42 text ,_43 text ,_44 text ,_47 text ,_49 text ,_5 text ,_50 text ,_51 text ,_52 text ,_53 text ,_54 text ,_55 text ,_6 text ,_663 text ,_664 text ,_666 text ,_667 text ,_669 text ,_7 text ,_76 text ,_766 text ,_767 text ,_77 text ,_770 text ,_772 text ,_773 text ,_774 text ,_775 text ,_8 text ,_80 text ,_81 text ,_82 text ,_823 text,_824 text,_83 text ,_84 text ,_85 text ,_855 text,_86 text ,_87 text ,_88 text ,_89 text ,_9 text ,_90 text 
     ,_91 text ,_910 text,_93 text ,_94 text ,_95 text ,_968 text,_98 text ,_99 text)
     )insert into etl_cidata
	select projectid, a.responselogid, distributionnodeid, olid, surveyid, surveydate, surveytime, surveytype, cycles, latitude, longitude, distributionnodetypeid, distributionnodename, distributionnodecode, approved, approveddate, rejected, rejectreason, resurveydone, responsestatusid, approverid, activityid, nextapproverroleid, locationid, nextapproverid, min_surveydate, min_surveytime 
    ,_1,_10,_100,_101,_102,_109,_11,_110,_111,_112,_113,_114,_115,_12,_13,_14,_15,_16,_17,_175,_176,_18,_180,_181,_19,_2,_20,_205,_206,_21,_210,_212,_213,_22,_23,_24,_25,_26,_27,_28,_29,_3,_30,_31,_317,_32,_33,_34,_35,_352,_355,_357,_36,_37,_372,_38,_382,_39,_4,_40,_41,_42,_43,_44,_47,_49,_5,_50,_51,_52,_53,_54,_55,_6,_663,_664,_666,_667,_669,_7,_76,_766,_767,_77,_770,_772,_773,_774,_775,_8,_80,_81,_82,_83,_84,_85,_86,_87,_88,_89,_9,_90,_91,_93,_94,_95,_98,_99,contactrid,_823,_824,_910,_855,_968
	from sdata a
	left join crosstabcte b on a.responselogid=b.responselogid;
--return 1;
    update tblresponselogs set updated=false where responselogid=$4;
    insert into etl_pushresponselogFORetl(responselogid,activityid,etlupdated) select $4,$3,false ;
    INSERT INTO etl_pushresponselogforetl_dashboard (responselogid, activityid, etlupdated) select $4,$3,false ;
    
    with timectea as(
    select max(tblapprovallogid) logid from tblapprovallogs 
    where responselogid=$4
    )
    ,detailctea as(
    select tblapprovallogid,logtime,userid from tblapprovallogs
    )
    select to_char(logtime,'YYYY-MM-DD HH:MI:SS')logtime1,userid from timectea a
    left join detailctea b on b.tblapprovallogid=a.logid into logstr;

     update tblresponselogs set sla_details=jsonb_build_object('updatedTimeStamp',logstr.logtime1,'updatedByUserRid',logstr.userid)
     where responselogid=$4;

--    update tbllots set status=0 where status=1 and roleid=68 and lotid not in(select distinct lotid from tblresponselogs where responsestatusid in(6));
--    update tbllots set status=0 where status=1 and roleid=10 and lotid not in(select distinct lotid from tblresponselogs where responsestatusid in(28));

/*
 --direct mi
    if respstatusid not in(1,3) then update tbldistributionnodes set lastactivityid=72 where distributionnodeid=distrid and lastactivityid<>72; 

    elsif respstatusid in(3) then update tbldistributionnodes set lastactivityid=0 where distributionnodeid=distrid and lastactivityid<>0;  

    elsif respstatusid=1 then update tbldistributionnodes set lastactivityid=-1 where distributionnodeid=distrid and lastactivityid<>-1 
    and distributionnodeid in(Select distnid from tblresponselogs where distributionnodeid=distrid and activityid=72 and responsestatusid=1 and projectid=13);

    end if;
--
*/
    END;

elsif $3 in(0,-1) then

	BEGIN

	delete from etl_masterdata where projectid=$1 and activityid=$3 and distributionnodetypeid=$2 and distributionnodeid=distrid;
	
	WITH MinSD AS (
	SELECT * FROM (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,SurveyDate,SurveyTime
			,(Row_Number() over (/*Partition BY s.distributionnodeid,s.activityid */ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID )) MaxNo
	FROM ( SELECT responselogid,projectid,activityid FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2) and B.activityid in($3)) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0 
		and d.distributionnodeid=distrid
	) s WHERE MaxNo=1
	)
	, Sdata AS (
	SELECT s.ProjectID,s.ResponseLogID,s.DistributionNodeID,OLID,SurveyID,s.SurveyDate,s.SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
	,DistributionNodeTypeID,DistributionNodeName,DistributionNodeCode,Approved,AppovedDate ApprovedDate,Rejected,RejectReason,ReSurveyDone,ResponseStatusID
	,ApproverID,s.ActivityID,NextApproverRoleID,LocationID,nextapproverid,s1.SurveyDate Min_SurveyDate,s1.SurveyTime Min_SurveyTime FROM (
	SELECT * FROM (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,OLID,SurveyID,SurveyDate,SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
			,s.ActivityID Survey_ActivityID
			,distributionnodename,distributionnodecode,DistributionNodeTypeID,UnderDistributionNodeID
			,(Row_Number() over (/*Partition BY s.distributionnodeid,s.activityid */ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID desc)) MaxNo
	FROM ( SELECT responselogid,projectid,responsedate,responsetime,approved,rejected,appoveddate,locationid,rejectreason,resurveydone,responsestatusid
			,approverid,nextapproverid,activityid,nextapproverroleid	FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2)   and B.activityid in($3) and distnid=distrid ) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0
	) s WHERE MaxNo=1 ) s
	LEFT JOIN MinSD s1 ON s1.DistributionNodeID=s.DistributionNodeID AND s1.activityid=s.activityid AND s1.projectid=s.projectid
	)
	,crosstabcte as (
	select *  from crosstab (
	'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
	cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
	where responselogid='||$4||' and nodetype='|| $2 ||' and items.categorypropertyallocationid in (18,19,16,17,15,80,5,6,4,2,81,21,82,83,24,98,766,84,85)',

	'select distinct ''_''|| categorypropertyallocationid 
	from tblcategorypropertyallocation q  
	right join tblproperties r on r.propertyid=q.propertyid where categoryid in(1,2)
	and categorypropertyallocationid in(18,19,16,17,15,80,5,6,4,2,81,21,82,83,24,98,766,84,85)
		ORDER  BY ''_''|| categorypropertyallocationid '
	 )
	 as newtable (
	  responselogid integer,_15 text,_16 text,_17 text,_18 text,_19 text,_2 text,_21 text,_24 text,_4 text,_5 text,_6 text,_766 text,_80 text,_81 text,_82 text,_83 text,_84 text,_85 text,_98 text
	 )
	)insert into etl_masterdata
	select a.*	,_15,_16,_17,_18,_19,_2,_21,_24,_4,_5,_6,_80,_81,_82,_83,_98,_766,_84,_85
	from sdata a
	left join crosstabcte b on a.responselogid=b.responselogid;
	--return 1;
	
	--merged Master
	delete from etl_merged_cimi_masterdata where projectid=$1 and activityid=$3 and distributionnodetypeid=$2 and distributionnodeid=distrid;
	
	WITH MinSD AS (
	SELECT * FROM (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,SurveyDate,SurveyTime
			,(Row_Number() over (/*Partition BY s.distributionnodeid,s.activityid */ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID )) MaxNo
	FROM ( SELECT responselogid,projectid,activityid FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2) and B.activityid in($3)) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0 
		and d.distributionnodeid=distrid
	) s WHERE MaxNo=1
	)
	, Sdata AS (
	SELECT s.ProjectID,s.ResponseLogID,s.DistributionNodeID,OLID,SurveyID,s.SurveyDate,s.SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
	,DistributionNodeTypeID,DistributionNodeName,DistributionNodeCode,Approved,AppovedDate ApprovedDate,Rejected,RejectReason,ReSurveyDone,ResponseStatusID
	,ApproverID,s.ActivityID,NextApproverRoleID,LocationID,nextapproverid,s1.SurveyDate Min_SurveyDate,s1.SurveyTime Min_SurveyTime FROM (
	SELECT * FROM (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,OLID,SurveyID,SurveyDate,SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
			,s.ActivityID Survey_ActivityID
			,distributionnodename,distributionnodecode,DistributionNodeTypeID,UnderDistributionNodeID
			,(Row_Number() over (/*Partition BY s.distributionnodeid,s.activityid */ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID desc)) MaxNo
	FROM ( SELECT responselogid,projectid,responsedate,responsetime,approved,rejected,appoveddate,locationid,rejectreason,resurveydone,responsestatusid
			,approverid,nextapproverid,activityid,nextapproverroleid	FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2)   and B.activityid in($3) and distnid=distrid ) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0
	) s WHERE MaxNo=1 ) s
	LEFT JOIN MinSD s1 ON s1.DistributionNodeID=s.DistributionNodeID AND s1.activityid=s.activityid AND s1.projectid=s.projectid
	)
	,crosstabcte as (
	select *  from crosstab (
	'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
	cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
	where responselogid='||$4||' and nodetype='|| $2 ||' and items.categorypropertyallocationid in (681,768,685,688,698,699,680,727,755,1071,1072,1073,1074,1075,654)',

	'select distinct ''_''|| categorypropertyallocationid 
	from tblcategorypropertyallocation q  
	right join tblproperties r on r.propertyid=q.propertyid where categoryid in(82,84,85)
	and categorypropertyallocationid in(681,768,685,688,698,699,680,727,755,1071,1072,1073,1074,1075,654)
		ORDER  BY ''_''|| categorypropertyallocationid '
	 )
	 as newtable (
	  responselogid integer,_1071 text ,_1072 text ,_1073 text ,_1074 text ,_1075 text ,_654 text ,_680 text ,_681 text ,_685 text ,_688 text ,_698 text ,_699 text ,_727 text ,_755 text ,_768 text	 )
	)insert into etl_merged_cimi_masterdata
	select a.*	,_1071,_1072,_1073,_1074,_1075,_654,_680,_681,_685,_688,_698,_699,_727,_755,_768
	from sdata a
	inner join crosstabcte b on a.responselogid=b.responselogid;

    --nsc master
    if($2=8) then
     WITH MinSD AS (
        SELECT * FROM (
        SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,SurveyDate,SurveyTime
                ,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID )) MaxNo
        FROM ( SELECT responselogid,projectid,activityid FROM tblresponselogs B WHERE b.ProjectID IN ($1)
                and b.nodetype in ($2) and B.activityid in($3)) r
        INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
        INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0 
            and d.distributionnodeid=distrid
        ) s WHERE MaxNo=1
        )
        , Sdata AS (
        SELECT s.ProjectID,s.ResponseLogID,s.DistributionNodeID,OLID,SurveyID,s.SurveyDate,s.SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
        ,DistributionNodeTypeID,DistributionNodeName,DistributionNodeCode,Approved,AppovedDate ApprovedDate,Rejected,RejectReason,ReSurveyDone,ResponseStatusID
        ,ApproverID,s.ActivityID,NextApproverRoleID,LocationID,nextapproverid,s1.SurveyDate Min_SurveyDate,s1.SurveyTime Min_SurveyTime,contactrid contact_rid FROM (
        SELECT * FROM (
        SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,OLID,SurveyID,SurveyDate,SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
                ,s.ActivityID Survey_ActivityID
                ,distributionnodename,distributionnodecode,DistributionNodeTypeID,UnderDistributionNodeID 
                ,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID desc)) MaxNo
        FROM ( SELECT responselogid,projectid,responsedate,responsetime,approved,rejected,appoveddate,locationid,rejectreason,resurveydone,responsestatusid
                ,approverid,nextapproverid,activityid,nextapproverroleid,contact_rid contactrid	FROM tblresponselogs B WHERE b.ProjectID IN ($1)
                and b.nodetype in ($2)   and B.activityid in($3) and distnid=distrid ) r
        INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
        INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0
        ) s WHERE MaxNo=1 ) s
        LEFT JOIN MinSD s1 ON s1.DistributionNodeID=s.DistributionNodeID AND s1.activityid=s.activityid AND s1.projectid=s.projectid
        )
        ,crosstabcte as (
        select *  from crosstab (
        'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
        cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
        where responselogid='||$4 ||' and activityid='||$3||' and nodetype='||$2||' and items.categorypropertyallocationid in (171,132,125,1142,137,129,1164,1165,1166,1167,1168,1169,1170,1171,1172,1173,163)
        ',

        'select distinct ''_''|| categorypropertyallocationid 
        from tblcategorypropertyallocation q  
        right join tblproperties r on r.propertyid=q.propertyid where 
         categorypropertyallocationid in(171,132,125,1142,137,129,1164,1165,1166,1167,1168,1169,1170,1171,1172,1173,163)
            ORDER  BY ''_''|| categorypropertyallocationid '
         ) 
        as newtable (
        responselogid integer,
         _1142 text,_1164 text,_1165 text,_1166 text,_1167 text,_1168 text,_1169 text,_1170 text,_1171 text,_1172 text,_1173 text,_125 text,_129 text,_132 text,_137 text,_163 text,_171 text)
        )insert into etl_nscmasterdata
        select projectid, a.responselogid, distributionnodeid, olid, surveyid, surveydate, surveytime, surveytype, cycles, latitude, longitude, distributionnodetypeid, distributionnodename, distributionnodecode, approved, approveddate, rejected, rejectreason, resurveydone, responsestatusid, approverid, activityid, nextapproverroleid, locationid, nextapproverid, min_surveydate, min_surveytime ,
       _1142,_1164,_1165,_1166,_1167,_1168,_1169,_1170,_1171,_1172,_1173,_125,_129,_132,_137,_163,_171,contact_rid
        from sdata a
        left join crosstabcte b on a.responselogid=b.responselogid;
    end if;	
        update tblresponselogs set updated=false where responselogid=$4;

   PERFORM push_dist_udf_master($4,$1);  
END;

elsif $3=10 then
BEGIN
   

       
       --sm 50 old
        if not exists(select 1 from tblsm_50 where distributionnodeid=distrid and valid=true)  then
               perform  fn_update_sm50status(distrid,$4);
        end if;
--sm 50 new
--        if not exists(select 1 from tblsm_50 where distributionnodeid=distrid and valid=true and serial_number in(select upper(value::jsonb->>'meter_Id')
--        from tblresponselogs
--        cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
--        where  responselogid=$4   and items.categorypropertyallocationid in (296)))  then
--               perform  fn_update_sm50status(distrid,$4);
--       
--        end if;

     if respstatusid in(0) and nextapproleid in(30) then  
                    insert into tp_meterinstall_searialno_push_logs (searialno,activityid,rl_rid,updated) 
                    select upper(_296::jsonb->>'meter_Id') ,$3,$4,false from etl_omdata a
                    inner join etl_warehouse_detailed w on w."Meter Serial No"=a._296::jsonb->>'meter_Id'
                    where a.responselogid=$4 and a._344 like ('Yes') and a._337 like ('Meter change');
     end if;


    select "valueId" ,serveyorid
    from tblresponselogs
    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int,"valueId" int4)
    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (343) into valu;
    
	select roleid from tblprojectuserallocation where userid=valu.serveyorid into roleidval;
	
	if roleidval=26 and respstatusid<>1 then
	BEGIN
    	if valu."valueId"=915 then update tblresponselogs set nextapproverroleid=29  where responselogid=$4;
    	elsif valu."valueId"=914 then update tblresponselogs set nextapproverroleid=30  where responselogid=$4;
    	end if ;
	END;
	end if;

    ---om new template
--    select "valueId" into  nextl2
--    from tblresponselogs
--    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int,"valueId" int4)
--    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (343) ;
--    select roleid from tblprojectuserallocation where userid=nextl2 and projectid=13 into nextrole;
--
--        if nextrole=28 and respstatusid<>1 then
--        BEGIN
--             update tblresponselogs set responsestatusid=30, nextapproverroleid=29  where responselogid=$4;
--        END;
--        end if;
--
--    select "valueId" into  submit
--    from tblresponselogs
--    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int,"valueId" int4)
--    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (385) ;
--
--        if submit = 995  then
--        BEGIN
--             UPDATE tblresponselogs SET responsestatusid = 0, nextapproverroleid = 29  where responselogid=$4 and responsestatusid<>1;
--        END;
--        end if;
--        if submit = 995 then
--            update se_assignment_master set status=3 where rl_rid=$4;
--        end if;
--
--        if exists(select 1 from se_assignment_master where rl_rid=$4 and status=3) then
--            update tblresponselogs set responsestatusid=1,nextapproverroleid=0,approved=1 where responselogid=$4;
--        end if;

---om new template----

--    select "valueId"   into valu1
--    from tblresponselogs
--    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int,"valueId" int4)
--    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (334) ;
--
--    if valu1=884 and respstatusid<>1 then 
--		update tblresponselogs set nextapproverroleid=30  where responselogid=$4; 
--    end if ;

---om new template
    select "valueId" into  nextl2
    from tblresponselogs
    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int,"valueId" int4)
    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (343) ;
    select roleid from tblprojectuserallocation where userid=nextl2 and projectid=13 into nextrole;
        if nextrole=28 and respstatusid<>1 then
        BEGIN
             update tblresponselogs set responsestatusid=30, nextapproverroleid=29  where responselogid=$4;
        END;
        end if;
    select "valueId" into  submit
    from tblresponselogs
    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int,"valueId" int4)
    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (385) ;
        if submit = 995  then
        BEGIN
             UPDATE tblresponselogs SET responsestatusid = 0, nextapproverroleid = 29  where responselogid=$4 and responsestatusid<>1;
        END;
        end if;
---om new template----


/*    
    select "valueId" into nextlab
    from tblresponselogs
    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int,"valueId" int4)
    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (1360) ;


if respstatusid=52  then 
    begin
		update tblresponselogs set responsestatusid=1,nextapproverroleid=74  where responselogid=$4; 
    end;
end if ;
--om lab ob
    select "valueId"   into lab_tesing_val
    from tblresponselogs
    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int,"valueId" int4)
    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (1360) ;
    
    if lab_tesing_val=1170 and nextapproleid=30 then 
        update tblresponselogs set responsestatusid=52  where responselogid=$4;
    end if;
*/
--om lab observation
    select "valueId"   into lab_tesing_val
    from tblresponselogs
    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int,"valueId" int4)
    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (1360) ;
    
    if lab_tesing_val=1170 and nextapproleid=30 then 
        update tblresponselogs set nextapproverroleid=10, nextapproverid=0  where responselogid=$4;
    end if;
 
    select value remarkse,serveyorid,concat(to_date(response::jsonb->>'surveyDate','dd-mm-yyyy'),' ',response::jsonb->>'surveyTime') sdate
    from tblresponselogs
    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (318) into valuse;
    
	select "FirstName" from tblusers where userid=valuse.serveyorid into seuser;
    
   	if exists (select externalticketid from se_assignment_master where rl_rid=$4 and externalticketid is not null) then
	BEGIN
    	if not exists (select context_rid from se_comments where context_rid=$4) then  INSERT INTO se_comments (context_type, context_rid, user_rid, comment_body, status,source,updated_by) select 2, $4, 1,valuse.remarkse, 1,'Helpdesk',seuser ; 
    	end if ;
	END;
	end if;

	PERFORM push_dist_udf($4,$1);

    if respstatusid in(1) then PERFORM push_dist_udf_master($4,$1); end if;

    delete from etl_omdata where projectid=$1 and activityid=$3 and distributionnodetypeid=$2 and distributionnodeid=distrid and responselogid=$4;

    WITH MinSD AS ( 
    select * from (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID
    ,coalesce(coalesce(surveydate,(case when response like '%surveyDate%' then response::jsonb->> 'surveyDate' else null end)),'') surveydate 
    ,coalesce((case when response like '%surveyTime%' then response::jsonb->> 'surveyTime' else null end),'') surveyTime 
	,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID )) MaxNo
	FROM ( SELECT responselogid,projectid,activityid,B.response FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2) and B.activityid in($3) and B.distnid=distrid and B.responselogid=$4) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0 and d.distributionnodeid=distrid
    ) as s where maxno=1
	)
	, Sdata AS (
	SELECT s.ProjectID,s.ResponseLogID,s.DistributionNodeID,OLID,SurveyID
        ,coalesce(coalesce(s.surveydate,(case when response like '%surveyDate%' then response::jsonb->> 'surveyDate' else null end)),'') surveydate 
        ,coalesce((case when response like '%surveyTime%' then response::jsonb->> 'surveyTime' else null end),'') surveyTime 
     ,SurveyType,Cycles,s.Latitude,s.Longitude
	,DistributionNodeTypeID,DistributionNodeName,DistributionNodeCode,Approved,AppovedDate ApprovedDate,Rejected,RejectReason,ReSurveyDone,ResponseStatusID
	,ApproverID,s.ActivityID,NextApproverRoleID,LocationID,nextapproverid,s1.SurveyDate Min_SurveyDate,s1.SurveyTime Min_SurveyTime,contact_rid FROM (
	SELECT * FROM (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,OLID,SurveyID,SurveyDate,SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
			,s.ActivityID Survey_ActivityID
			,distributionnodename,distributionnodecode,DistributionNodeTypeID,UnderDistributionNodeID 
			,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID desc)) MaxNo
	FROM ( SELECT responselogid,projectid,responsedate,responsetime,approved,rejected,appoveddate,locationid,rejectreason,resurveydone,responsestatusid
			,approverid,nextapproverid,activityid,nextapproverroleid,contact_rid	FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2)   and B.activityid in($3) and distnid=distrid and B.responselogid=$4) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0
	) s where maxno=1 ) s
	LEFT JOIN MinSD s1 ON s1.DistributionNodeID=s.DistributionNodeID AND s1.activityid=s.activityid AND s1.projectid=s.projectid
	)
   ,crosstabcte as (
	select *  from crosstab (
	'select responselogid,''_''||categorypropertyallocationid,string_agg(value::text,'','')::text val from tblresponselogs
	cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
	where responselogid='||$4||' and nodetype='|| $2 ||' and items.categorypropertyallocationid in (291,305,326,327,328,331,214,215,216,218,219,222,223,224,225,226,227,228,230,232,233,234,236,239,240,241,242,243,244,246,247,248,249,250,253,256,258,259,262,266,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,292
   ,293,294,296,298,318,325,329,330,334,335,336,337,338,339,340,341,342,343,344,345,346,347,383,384)
    group by responselogid,''_''||categorypropertyallocationid',

	'select distinct ''_''|| categorypropertyallocationid 
	from tblcategorypropertyallocation q  
	right join tblproperties r on r.propertyid=q.propertyid where categoryid in(32)
	and categorypropertyallocationid in(291,305,326,327,328,331,214,215,216,218,219,222,223,224,225,226,227,228,230,232,233,234,236,239,240,241,242,243,244,246,247,248,249,250,253,256,258,259,262,266,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,292,293,294,296,298,318,325,329,330,334,335,336,337,338,339,340,341,342,343,344,345,346,347,383,384
     )
    ORDER  BY ''_''|| categorypropertyallocationid '
	 )
	 as newtable (responselogid integer,_214 text,_215 text,_216 text,_218 text,_219 text,_222 text,_223 text,_224 text,_225 text,_226 text,_227 text,_228 text,_230 text,_232 text,_233 text,_234 text,_236 text,_239 text,_240 text,_241 text,_242 text,_243 text,_244 text,_246 text,_247 text,_248 text,_249 text,_250 text,_253 text,_256 text,_258 text,_259 text,
    _262 text,_266 text,_269 text,_270 text,_271 text,_272 text,_273 text,_274 text,_275 text,_276 text,_277 text,_278 text,_279 text,_280 text,_281 text,_282 text,_283 text,_284 text,_285 text,_286 text,_291 text,_292 text,_293 text,_294 text,_296 text,_298 text,_305 text,_318 text,_325 text,_326 text,_327 text,_328 text,_329 text,_330 text,_331 text,_334 text,
    _335 text,_336 text,_337 text,_338 text,_339 text,_340 text,_341 text,_342 text,_343 text,_344 text,_345 text,_346 text,_347 text,_383 text,_384 text)
    )insert into etl_omdata
      select projectid, a.responselogid, distributionnodeid, olid, surveyid, surveydate, surveytime, surveytype, cycles, latitude, longitude, distributionnodetypeid, distributionnodename, distributionnodecode, approved, approveddate, rejected, rejectreason, resurveydone, responsestatusid, approverid, activityid, nextapproverroleid, locationid, nextapproverid, min_surveydate, min_surveytime 
     ,_214,_215,_216,_218,_219,_222,_223,_224,_225,_226,_227,_228,_230,_232,_233,_234,_236,_239,_240,_241,_242,_243,_244,_246,_247,_248,_249,_250,_253,_256,_258,_259,_262,_266,_269,_270,_271,_272,_273,_274,_275,_276,_277
     ,_278,_279,_280,_281,_282,_283,_284,_285,_286,_292,_293,_294,_296,_298,_318,_325,_329,_330,_334,_335,_336,_337,_338,_339,_340,_341,_342,_343,_344,_345,_346,_347,_383,_384,_291,_305,_326,_327,_328,_331,contact_rid
	from sdata a
	left join crosstabcte b on a.responselogid=b.responselogid;
	 
    update tblresponselogs set updated=false where responselogid=$4;

    with timectea as(
    select max(tblapprovallogid) logid from tblapprovallogs 
    where responselogid=$4
    )
    ,detailctea as(
    select tblapprovallogid,logtime,userid from tblapprovallogs
    )
    select to_char(logtime,'YYYY-MM-DD HH:MI:SS')logtime1,userid from timectea a
    left join detailctea b on b.tblapprovallogid=a.logid into logstr;

    update tblresponselogs set sla_details=jsonb_build_object('updatedTimeStamp',logstr.logtime1,'updatedByUserRid',logstr.userid) where responselogid=$4;

    insert into etl_pushslnoforetl (slno,etlupdated) select _296::jsonb->>'meter_Id' ,false from etl_omdata where responselogid=$4;

    select response::jsonb->>'amRid' into amidtemp from tblresponselogs where responselogid=$4;
    if amidtemp=0 then
    BEGIN
        select am_rid into amid from se_assignment_master where rl_rid=$4;
        update se_assignment_master set status=3 where am_rid=amid;
        delete from se_assignment_user_map where am_rid=amid;
        str='update tblresponselogs set response=jsonb_set(response::jsonb,''{amRid}'',''"'||amid ||'"'' ,true) where responselogid='||$4;
        execute str;
    END;
    end if;

     if respstatusid in(1)--0 and nextapproleid in(30)
      then  
                    insert into tp_meterinstall_searialno_push_logs (searialno,activityid,rl_rid,updated)
                    select upper(_296::jsonb->>'meter_Id') ,$3,$4,false from etl_omdata a
                    --inner join etl_warehouse_detailed w on w."Meter Serial No"=a._296::jsonb->>'meter_Id'
                    where a.responselogid=$4 and a._344 like ('Yes') and a._337 like ('Meter change');
                    insert into tp_meterinstall_searialno_push_logs_mdmaccenture (searialno,activityid,rl_rid,updated)
                select upper(_296::jsonb->>'meter_Id') ,$3,$4,false from etl_omdata a
                where a.responselogid=$4;
     end if;


END;

--DT O&M
elsif $3=11 then
    BEGIN
   
    select "valueId" ,serveyorid
    from tblresponselogs
    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int,"valueId" int4)
    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (1215) into valu;-- Assign To

	select value 
    from tblresponselogs
    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (1259,1271,1246,1281) and value<>'OTHERS' into reason;--Others Remarks - MC,Others Remarks-NIC,Others Remarks-SIM,Others Remarks

    select roleid from tblprojectuserallocation where userid=valu.serveyorid into roleidval;

	if roleidval=79 and respstatusid<>1 then--assign to property values
        BEGIN
            if valu."valueId"=20948 then update tblresponselogs set nextapproverroleid=29,rejectreason=reason  where responselogid=$4;--next level
            elsif valu."valueId"=20947 then update tblresponselogs set nextapproverroleid=30  where responselogid=$4;--Self
            end if ;
        END;
	end if;

	if roleidval=79 and respstatusid=0 then
        BEGIN
            if valu."valueId"=20948 then update tblresponselogs set responsestatusid=30   where responselogid=$4;
            end if ;
        END;
	end if;

    select "valueId" into valu1
    from tblresponselogs
    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int,"valueId" int4)
    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (1214) ;
      
    if valu1=20945 and respstatusid<>1 then 
		update tblresponselogs set nextapproverroleid=30  where responselogid=$4; 
    end if ;

    --om lab
    if respstatusid=52 and nextapproleid=30 then 
        begin
            update tblresponselogs set responsestatusid=1,nextapproverroleid=74  where responselogid=$4; 
        end;
    end if ;

    select value remarkse,serveyorid,concat(to_date(response::jsonb->>'surveyDate','dd-mm-yyyy'),' ',response::jsonb->>'surveyTime') sdate
    from tblresponselogs
    cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
    where response<>'' and responselogid=$4   and items.categorypropertyallocationid in (1245) into valuse;

	select "FirstName" from tblusers where userid=valuse.serveyorid into seuser;

	PERFORM push_dist_udf($4,$1);

    if respstatusid in(1) then PERFORM push_dist_udf_master($4,$1); end if;

    with timectea as(
    select max(tblapprovallogid) logid from tblapprovallogs 
    where responselogid=$4
    )
    ,detailctea as(
    select tblapprovallogid,logtime,userid from tblapprovallogs
    )
    select to_char(logtime,'YYYY-MM-DD HH:MI:SS')logtime1,userid from timectea a
    left join detailctea b on b.tblapprovallogid=a.logid into logstr;

	--dt 0&m close	
    select response::jsonb->>'amRid' into amidtemp from tblresponselogs where responselogid=$4;
    if amidtemp=0 then
    BEGIN
        select am_rid into amid from se_assignment_master where rl_rid=$4;
        update se_assignment_master set status=3 where am_rid=amid;
        delete from se_assignment_user_map where am_rid=amid;
        str='update tblresponselogs set response=jsonb_set(response::jsonb,''{amRid}'',''"'||amid ||'"'' ,true) where responselogid='||$4;
        execute str;
    END;
    end if; 

    delete from etl_dtomdata where projectid=$1 and activityid=$3 and distributionnodetypeid=$2 and distributionnodeid=distrid and responselogid=$4;

    WITH MinSD AS ( 
    select * from (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID
    ,coalesce(coalesce(surveydate,(case when response like '%surveyDate%' then response::jsonb->> 'surveyDate' else null end)),'') surveydate 
    ,coalesce((case when response like '%surveyTime%' then response::jsonb->> 'surveyTime' else null end),'') surveyTime 
	,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID )) MaxNo
	FROM ( SELECT responselogid,projectid,activityid,B.response FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2) and B.activityid in($3) and B.distnid=distrid and B.responselogid=$4) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0 and d.distributionnodeid=distrid
    ) as s where maxno=1
	)
	, Sdata AS (
	SELECT s.ProjectID,s.ResponseLogID,s.DistributionNodeID,OLID,SurveyID
        ,coalesce(coalesce(s.surveydate,(case when response like '%surveyDate%' then response::jsonb->> 'surveyDate' else null end)),'') surveydate 
        ,coalesce((case when response like '%surveyTime%' then response::jsonb->> 'surveyTime' else null end),'') surveyTime 
     ,SurveyType,Cycles,s.Latitude,s.Longitude
	,DistributionNodeTypeID,DistributionNodeName,DistributionNodeCode,Approved,AppovedDate ApprovedDate,Rejected,RejectReason,ReSurveyDone,ResponseStatusID
	,ApproverID,s.ActivityID,NextApproverRoleID,LocationID,nextapproverid,s1.SurveyDate Min_SurveyDate,s1.SurveyTime Min_SurveyTime,contact_rid FROM (
	SELECT * FROM (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,OLID,SurveyID,SurveyDate,SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
			,s.ActivityID Survey_ActivityID
			,distributionnodename,distributionnodecode,DistributionNodeTypeID,UnderDistributionNodeID 
			,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID desc)) MaxNo
	FROM ( SELECT responselogid,projectid,responsedate,responsetime,approved,rejected,appoveddate,locationid,rejectreason,resurveydone,responsestatusid
			,approverid,nextapproverid,activityid,nextapproverroleid,contact_rid	FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2)   and B.activityid in($3) and distnid=distrid and B.responselogid=$4) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0
	) s where maxno=1 ) s
	LEFT JOIN MinSD s1 ON s1.DistributionNodeID=s.DistributionNodeID AND s1.activityid=s.activityid AND s1.projectid=s.projectid
	)
   ,crosstabcte as (
	select *  from crosstab (
	'select responselogid,''_''||categorypropertyallocationid,string_agg(value::text,'','')::text val from tblresponselogs
	cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
	where responselogid='||$4||' and nodetype='|| $2 ||' and items.categorypropertyallocationid in (1280,1281,1233,1247,1256,1232,1217,1239,1269,1267,1249,1245,1277,1211,1229,1261,1230,1208,1284,1274,1234,1226,1278,1224,1241,1272,1248,1244,1209,1259,1220,1219,1213,1265,1254,1273,1286,1285,1237,1268,1222,1271,1252,1264,1236,1263,1215,1216,1178,1210,1279,1258,1225,1240,1223,1238,1260,
    1212,1250,1257,1283,1218,1282,1243,1214,1246,1221,1227,1276,1270,1287,1275,1253)
    group by responselogid,''_''||categorypropertyallocationid',

	'select distinct ''_''|| categorypropertyallocationid 
	from tblcategorypropertyallocation q   
	where categorypropertyallocationid in(1280,1281,1233,1247,1256,1232,1217,1239,1269,1267,1249,1245,1277,1211,1229,1261,1230,1208,1284,1274,1234,1226,1278,1224,1241,1272,1248,1244,1209,1259,1220,1219,1213,1265,1254,1273,1286,1285,1237,1268,1222,1271,1252,1264,1236,1263,1215,1216,1178,1210,1279,1258,1225,1240,1223,1238,1260,
    1212,1250,1257,1283,1218,1282,1243,1214,1246,1221,1227,1276,1270,1287,1275,1253)
		ORDER  BY ''_''|| categorypropertyallocationid '
	 )
	 as newtable (  responselogid integer,  _1178 text , _1208 text , _1209 text , _1210 text , _1211 text , _1212 text , _1213 text , _1214 text , _1215 text , _1216 text , _1217 text , _1218 text , _1219 text , _1220 text , _1221 text , _1222 text , _1223 text , 
     _1224 text , _1225 text , _1226 text , _1227 text , _1229 text , _1230 text , _1232 text , _1233 text , _1234 text , _1236 text , _1237 text , _1238 text , _1239 text , _1240 text , _1241 text , _1243 text , _1244 text , _1245 text , _1246 text , _1247 text ,
     _1248 text , _1249 text , _1250 text , _1252 text , _1253 text , _1254 text , _1256 text , _1257 text , _1258 text , _1259 text , _1260 text , _1261 text , _1263 text , _1264 text , _1265 text , _1267 text , _1268 text , _1269 text , _1270 text , _1271 text , _1272 text , _1273 text , _1274 text , _1275 text , _1276 text , _1277 text , _1278 text ,
     _1279 text , _1280 text , _1281 text , _1282 text , _1283 text , _1284 text , _1285 text , _1286 text , _1287 text)
    )insert into etl_dtomdata
      select projectid, a.responselogid, distributionnodeid, olid, surveyid, surveydate, surveytime, surveytype, cycles, latitude, longitude, distributionnodetypeid, distributionnodename, distributionnodecode, approved, approveddate, rejected, rejectreason, resurveydone, responsestatusid, approverid, activityid, nextapproverroleid, locationid, nextapproverid, min_surveydate, min_surveytime 
     ,contact_rid,_1178,_1208,_1209,_1210,_1211,_1212,_1213,_1214,_1215,_1216,_1217,_1218,_1219,_1220,_1221,_1222,_1223,_1224,_1225,_1226,_1227,_1229,_1230,_1232,_1233,_1234,_1236,_1237,_1238,_1239,_1240,_1241,_1243,_1244,_1245,_1246,_1247,_1248,_1249,_1250,_1252,_1253,_1254,_1256,_1257,_1258,_1259,_1260,_1261,_1263,_1264,_1265,_1267,_1268,_1269,_1270,_1271,_1272,_1273,
    _1274,_1275,_1276,_1277,_1278,_1279,_1280,_1281,_1282,_1283,_1284,_1285,_1286,_1287
	from sdata a
	left join crosstabcte b on a.responselogid=b.responselogid;

																							
   update tblresponselogs set sla_details=jsonb_build_object('updatedTimeStamp',logstr.logtime1,'updatedByUserRid',logstr.userid) where responselogid=$4;														
   update tblresponselogs set updated=false where responselogid=$4;													
END;		
												
--dtmi etl
elsif $3 in(44) then
  
    BEGIN
   
    update tblresponselogs set nextapproverroleid=10 ,nextapproverid=0 where responselogid=$4 and responsestatusid=28 and nextapproverroleid=0;
   
    delete from etl_dtmi where projectid=$1 and activityid=$3 and distributionnodetypeid=$2 and distnid=distrid;

    --Ping test
    if resurveyid = 1 AND respstatusid = 3 THEN
        DELETE FROM tblresurveypropertyvaluedetails 
        WHERE resurveypropertyvaluedetailsid IN (
            SELECT DISTINCT d.resurveypropertyvaluedetailsid
            FROM tblresurvey a
            LEFT JOIN tblresurveydetaails b 
                   ON a.resurveyid = b.resurveyid
            INNER JOIN tblresurveyproperties c 
                   ON b.resurveydetailsid = c.resurveydetailsid
            INNER JOIN tblresurveypropertyvaluedetails d 
                   ON c.resurveypropertyid = d.resurveypropertyid
            WHERE a.responseid = $4
              AND c.catogorypropertyallocationid IN (1424, 1425, 1426)
        );
    end if;
    ----ping test--

 
    WITH MinSD AS (
	  SELECT *  FROM (
	   SELECT r.*
			,(Row_Number() over (Partition BY distnid,activityid ORDER BY distnid,activityid,responselogid )) mindate
	   FROM ( SELECT responselogid,projectid,activityid,distnid,B.surveydate,b.responsetime FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2) and B.activityid in($3) 	and b.distnid=distrid and B.responsestatusid>=0) r
	
	  ) s WHERE mindate=1
    )--select * from minsd

    ,Sdata AS (
	SELECT s.ProjectID,s.ResponseLogID,s.distnid,serveyorid,s.SurveyDate,SurveyType,cycle,s.Latitude,s.Longitude
	,DistributionNodeTypeID,DistributionNodeName,DistributionNodeCode,Approved,AppovedDate ApprovedDate,Rejected,RejectReason,ReSurveyDone,ResponseStatusID
	,ApproverID,s.ActivityID,NextApproverRoleID,LocationID,nextapproverid,s1.SurveyDate Min_SurveyDate,s1.responsetime min_surveytime,contact_rid FROM (

	SELECT r.* ,distributionnodename,distributionnodecode,DistributionNodeTypeID,UnderDistributionNodeID,cycle ,
    (Row_Number() over (Partition BY distnid,activityid ORDER BY distnid,activityid,responselogid desc)) MaxNo
	FROM ( SELECT responselogid,projectid,responsedate,responsetime,B.distnid,B.serveyorid,B.surveydate,B.surveytype,B.latitude,B.longitude,
          approved,rejected,appoveddate,locationid,rejectreason,resurveydone,B.responsestatusid,approverid,nextapproverid,b.activityid,
          nextapproverroleid,contact_rid	FROM tblresponselogs B WHERE b.ProjectID IN ($1) and B.responsestatusid>=0
			and b.nodetype in ($2)   and B.activityid in($3) and distnid=distrid ) r
	
        INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=r.distnid AND COALESCE(deleted,0)=0
        ) s
       inner JOIN MinSD s1 ON s1.distnid=s.distnid AND s1.activityid=s.activityid AND s1.projectid=s.projectid
        where MaxNo=1
    )--select * from Sdata

    ,crosstabcte as (
	select *  from crosstab (
	'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
	cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
	where responselogid='||$4||' and nodetype='|| $2 ||' and items.categorypropertyallocationid in(494,497,498,499,512,525,526,527,530,531,532,534,536,538,540,541,543,551,552,553,555,556,557,558,559,560,561,562,564,565,566,567,568,569,570,571,572,573,574,575,576,577,579,
    580,638,662,674,788,799,800,801,802,803,804,805,808,984,987,988,989,796,1034,1068,807,806,1032,1033,1031,985,983,1044,1066,1048,1049,1050,1052,1053,1054,1055,1057,1058,1019,1018,1020)',

	'select distinct ''_''|| categorypropertyallocationid 
	from tblcategorypropertyallocation q  
	right join tblproperties r on r.propertyid=q.propertyid where categoryid in(78,77,76,75,91)
	and categorypropertyallocationid in(494,497,498,499,512,525,526,527,530,531,532,534,536,538,540,541,543,551,552,553,555,556,557,558,559,560,561,562,564,565,566,567,568,569,570,571,572,573,574,575,576,577,579,
    580,638,662,674,788,799,800,801,802,803,804,805,808,984,987,988,989,796,1034,1068,807,806,1032,1033,1031,985,983,1044,1066,1048,1049,1050,1052,1053,1054,1055,1057,1058,1019,1018,1020)
		ORDER  BY ''_''|| categorypropertyallocationid '
	 )
	 as newtable (
	  responselogid integer,_1018 text,_1019 text,_1020 text,_1031 text,_1032 text,_1033 text,_1034 text,_1044 text,_1048 text,_1049 text,_1050 text,_1052 text,_1053 text,_1054 text,_1055 text,_1057 text,_1058 text,_1066 text,_1068 text,_494 text,  _497 text,  _498 text,  _499 text,  _512 text,  _525 text,  _526 text,  _527 text,  _530 text,  _531 text,  _532 text,  _534 text,  _536 text,  _538 text,  _540 text,  _541 text,  _543 text,  _551 text,  _552 text,  _553 text,  _555 text,  _556 text,  _557 text,  _558 text,  _559 text,  _560 text,  _561 text,  _562 text,  _564 text,  _565 text,  _566 text,  _567 text,  _568 text,  _569 text,  _570 text,  _571 text,  _572 text,  _573 text,  _574 text,  _575 text,  _576 text,  _577 text,  _579 text,  _580 text,  _638 text,  _662 text,  _674 text,  _788 text,_796 text,  _799 text,  _800 text,  _801 text,  _802 text,  _803 text,  _804 text,  _805 text,_806 text,_807 text,  _808 text,_983 text,  _984 text,_985 text,  _987 text,  _988 text,  _989 text)
	 )insert into etl_dtmi
        select projectid, a.responselogid, distnid, locationid, serveyorid, surveydate, surveytype, cycle, latitude, longitude, distributionnodetypeid, distributionnodename, distributionnodecode, approved, approveddate, rejected, rejectreason, resurveydone, responsestatusid, approverid, activityid, nextapproverroleid, nextapproverid,
       min_surveydate,min_surveytime,contact_rid, _494,_497,_498,_499,_512,_525,_526,_527,_530,_531,_532,_534,_536,_538,_540,_541,_543,_551,_552,_553,_555,_556,_557,_558,_559,_560,_561,_562,_564,_565,_566,_567,_568,_569,_570,_571,_572,_573,_574,_575,_576,_577,_579,_580,_638,_662,_674,_788,_799,_800,_801,_802,_803,_804,_805,_808,_984,_987,_988,_989,_985,_983,_796,_806,_807,
       _1031,_1032,_1033,_1034,_1068,_1044,_1048,_1049,_1050,_1052,_1053,_1054,_1055,_1057,_1058,_1066,_1018,_1019,_1020
      from sdata a 
     left join crosstabcte b on a.responselogid=b.responselogid;

    if respstatusid in(1) then PERFORM push_dist_udf_master($4,$1); end if;

    if respstatusid in(1) then PERFORM push_dist_udf($4,$1); end if;
	update tblresponselogs set updated=false where responselogid=$4;
    
   insert into etl_pushresponselogFORetl(responselogid,activityid,etlupdated) select $4,$3,false ;

   insert into etl_pushslnoforetl (slno,etlupdated) select _552::jsonb->>'meter_Id' ,false from etl_dtmi where responselogid=$4;

--package 2 insert 
 if not exists (select 1 from tbl_distinid_package where distinid=distrid) then 
        INSERT INTO tbl_distinid_package (distinid, activityid, activity_package, updateddatetime) 
        select a.distnid,a.activityid,b.warehouse_package,now() from  etl_dtmi a join tbl_meterno_package b
        on upper(a._552::jsonb->>'meter_Id')=b.serialno where a.responselogid=$4;
end if ;
 END;
      

elsif $3 in(43) then

	BEGIN
	
        update tblresponselogs set nextapproverroleid=10 ,nextapproverid=0 where responselogid=$4 and responsestatusid=28 and nextapproverroleid=0;
     --updation

        select value into indtcode
        from tblresponselogs a2
        cross join lateral jsonb_to_recordset(a2.response::jsonb->'propertiesBean') as items(categorypropertyallocationid int4,value text)  
        where  categorypropertyallocationid in (804) and activityid=$3 
        and responselogid in ($4) and response<>'' ;

        select value into indtname
        from tblresponselogs a2
        cross join lateral jsonb_to_recordset(a2.response::jsonb->'propertiesBean') as items(categorypropertyallocationid int4,value text)  
        where  categorypropertyallocationid in (806) and activityid=$3 and projectid=$1
        and responselogid in ($4) and response<>'' ;

        select distributionnodecode,distributionnodename  from tbldistributionnodes where distributionnodetypeid is not null and distributionnodeid=distrid into dnamedt;
    
    if not exists(select d_ic_id from tbldistribution_intelismart_code where distid=distrid and new_dist_code=indtcode  and rl_rid=$4)then
    begin
        
        select surveytype into stype from tblresponselogs where projectid=$1 and activityid=$3 and distnid=distrid;

        if (stype='Existing') then
            begin
                update tblresponselogs set distname=dnamedt.distributionnodename,distcode=indtcode where activityid=$3 and responselogid=$4;
                update tbldistributionnodes set distributionnodecode=indtcode  where distributionnodetypeid is not null and distributionnodeid=distrid;
            end;
        elsif(stype='New') then
            begin
                update tblresponselogs set distname=(case when coalesce(indtname,'')='' then '' else indtname end ),distcode=indtcode where activityid=$3 and responselogid=$4;
                update tbldistributionnodes set distributionnodecode=indtcode,distributionnodename=(case when coalesce(indtname,'')='' then '' else indtname end )  where distributionnodetypeid is not null and distributionnodeid=distrid;
            end;
        end if;

        insert into tbldistribution_intelismart_code (distid,old_dist_code,new_dist_code,activityid,rl_rid) values (distrid,dnamedt.distributionnodecode,indtcode,$3,$4);

    end;
    end if;

     --dtci etl
    delete from etl_dtci where projectid=$1 and activityid=$3 and distributionnodetypeid=$2 and distnid=distrid;
 

   WITH MinSD AS (
	 SELECT *  FROM (
	   SELECT r.*
			,(Row_Number() over (Partition BY distnid,activityid ORDER BY distnid,activityid,responselogid )) MaxNo
	   FROM ( SELECT responselogid,projectid,activityid,distnid,B.surveydate,b.responsetime FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2) and B.activityid in($3) 	and b.distnid=distrid) r
	
	 ) s WHERE MaxNo=1
   )--select * from minsd

  ,Sdata AS (
	SELECT s.ProjectID,s.ResponseLogID,s.distnid,serveyorid,s.SurveyDate,SurveyType,cycle,s.Latitude,s.Longitude
	,DistributionNodeTypeID,DistributionNodeName,DistributionNodeCode,Approved,AppovedDate ApprovedDate,Rejected,RejectReason,ReSurveyDone,ResponseStatusID
	,ApproverID,s.ActivityID,NextApproverRoleID,LocationID,nextapproverid,s1.SurveyDate Min_SurveyDate,s1.responsetime min_surveytime,contact_rid FROM (

	SELECT r.* ,distributionnodename,distributionnodecode,DistributionNodeTypeID,UnderDistributionNodeID,cycle ,
    (Row_Number() over (Partition BY distnid,activityid ORDER BY distnid,activityid,responselogid desc)) MaxNo
	FROM ( SELECT responselogid,projectid,responsedate,responsetime,B.distnid,B.serveyorid,B.surveydate,B.surveytype,B.latitude,B.longitude,
          approved,rejected,appoveddate,locationid,rejectreason,resurveydone,B.responsestatusid,approverid,nextapproverid,b.activityid,
          nextapproverroleid,contact_rid	FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2)   and B.activityid in($3) and distnid=distrid ) r
	
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=r.distnid AND COALESCE(deleted,0)=0
    ) s
   inner JOIN MinSD s1 ON s1.distnid=s.distnid AND s1.activityid=s.activityid AND s1.projectid=s.projectid 
	where s.maxno=1
  )--select * from Sdata

  ,crosstabcte as (
	select *  from crosstab (
	'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
	cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
	where responselogid='||$4||' and nodetype='|| $2 ||' and items.categorypropertyallocationid in(494,497,498,499,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517
    ,518,519,520,521,522,524,525,526,527,528,529,530,531,532,534,536,538,540,541,542,543
    ,545,546,547,548,549,550,638,662,665,673,782,783,784,787,788,794,795,796,797,798
    ,799,800,801,802,803,804,805,806,807,808,825,826)',

	'select distinct ''_''|| categorypropertyallocationid 
	from tblcategorypropertyallocation q  
	right join tblproperties r on r.propertyid=q.propertyid where categoryid in(75,76,77)
	and categorypropertyallocationid in(494,497,498,499,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517
    ,518,519,520,521,522,524,525,526,527,528,529,530,531,532,534,536,538,540,541,542,543
    ,545,546,547,548,549,550,638,662,665,673,782,783,784,787,788,794,795,796,797,798
    ,799,800,801,802,803,804,805,806,807,808,825,826)
            ORDER  BY ''_''|| categorypropertyallocationid '
	 )
	 as newtable (
	  responselogid integer,_494 text,_497 text,_498 text,_499 text,_501 text,_502 text,_503 text,_504 text,_505 text,_506 text,_507 text,_508 text,_509 text,_510 text,_511 text,_512 text,_513 text,_514 text,_515 text,_516 text,_517
      text,_518 text,_519 text,_520 text,_521 text,_522 text,_524 text,_525 text,_526 text,_527 text,_528 text,_529 text,_530 text,_531 text,_532 text,_534 text,_536 text,_538 text,_540 text,_541 text,_542 text,_543
      text,_545 text,_546 text,_547 text,_548 text,_549 text,_550 text,_638 text,_662 text,_665 text,_673 text,_782 text,_783 text,_784 text,_787 text,_788 text,_794 text,_795 text,_796 text,_797 text,_798
      text,_799 text,_800 text,_801 text,_802 text,_803 text,_804 text,_805 text,_806 text,_807 text,_808 text,_825 text,_826 text)
	 )insert into etl_dtci
	select projectid, a.responselogid, distnid, locationid, serveyorid, surveydate, surveytype, cycle, latitude, longitude, distributionnodetypeid, distributionnodename, distributionnodecode, approved, approveddate, rejected, rejectreason, resurveydone, responsestatusid, approverid, activityid, nextapproverroleid, nextapproverid,
    min_surveydate,min_surveytime,contact_rid,_494,_497,_498,_499,_501,_502,_503,_504,_505,_506,_507,_508,_509,_510,_511,_512,_513,_514,_515,_516,_517
    ,_518,_519,_520,_521,_522,_524,_525,_526,_527,_528,_529,_530,_531,_532,_534,_536,_538,_540,_541,_542,_543
    ,_545,_546,_547,_548,_549,_550,_638,_662,_665,_673,_782,_783,_784,_787,_788,_794,_795,_796,_797,_798
    ,_799,_800,_801,_802,_803,_804,_805,_806,_807,_808,_825,_826
    from sdata a
	left join crosstabcte b on a.responselogid=b.responselogid;

    ---direct dtmi
    if respstatusid not in(1,3) then update tbldistributionnodes set lastactivityid=44 where distributionnodeid=distrid and lastactivityid<>44; 

    elsif respstatusid in(3) then update tbldistributionnodes set lastactivityid=0 where distributionnodeid=distrid and lastactivityid<>0;  

    elsif respstatusid=1 then update tbldistributionnodes set lastactivityid=-1 where distributionnodeid=distrid and lastactivityid<>-1 
    and distributionnodeid in(Select distnid from tblresponselogs where distributionnodeid=distrid and activityid=44 and responsestatusid=1 and projectid=9);
    end if;
--

     PERFORM push_dist_udf_master($4,$1); 
	update tblresponselogs set updated=false where responselogid=$4;
    
    insert into etl_pushresponselogFORetl(responselogid,activityid,etlupdated) select $4,$3,false ;

	PERFORM push_dist_udf_master($4,$1);
   END;
	

elsif $3=73 then

    BEGIN
         PERFORM push_dist_udf($4,$1);
  
  
      --Consumer denied Access
         select "valueId" into C_denied_A
         from tblresponselogs
         cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items(value text, "valueId" int,categorypropertyallocationid int)
         where activityid=$3 and projectid=$1 and responselogid=$4 and items.categorypropertyallocationid in (660) and value='Consumer denied access' and response<>'';

         if C_denied_A=16285 then
            BEGIN
             update tblresponselogs set responsestatusid=27,nextapproverroleid=0,approved=1  where responselogid=$4 and activityid=$3;
             update tbldistributionnodes set lastactivityid=-1 where distributionnodeid=distrid and distributionnodetypeid=$2;
            END;
         end if;

--buckets
    select roleid into holdroleid from tblprojectuserallocation where userid in(Select nextapproverid from tblresponselogs where responselogid=$4 ) and projectid=13;

    if nextapproleid in(9,68,10) and respstatusid not in(2,7)/*or holdroleid in(9,68,10)*/ then
    BEGIN
           --sm 50
        if $3=73 and (nextapproleid=9 or holdroleid=9) then --and not exists(select 1 from tblsm_50 where distributionnodeid=distrid and valid=true)  then
              -- perform  fn_update_sm50status(distrid,$4); 
       -- else
            --4 level bucket
            BEGIN    
            select billread,to_date(billdate,'dd-MM-yyyy') billdate 
            from (
            select row_number() over(order by discombillreadid desc) rn,billread ,billdate
            from tbldiscombillreads where activityid=73 and distnid=distrid) as P
            where rn=1 into mibillinfo;

            select value into kwhmi
            from tblresponselogs
            cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
            where responselogid=$4 and items.categorypropertyallocationid in (740);

            select to_date(surveydate,'dd/MM/yyyy') into midate from tblresponselogs where responselogid=$4;

            select value into oldmetstatcode
            from tblresponselogs
            cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
            where responselogid=$4 and items.categorypropertyallocationid in (732);

            select value into billtype
            from tblresponselogs
            cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
            where responselogid=$4 and items.categorypropertyallocationid in (694);

            select value into importkwh
            from tblresponselogs
            cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
            where responselogid=$4 and items.categorypropertyallocationid in (1105);


            update tblresponselogs set responsestatusid=(case when oldmetstatcode='Faulty' then 29 when mibillinfo.billread is null then 24
                                                        when billtype in ('Solar Meter') and (importkwh-mibillinfo.billread)<0 then 22 
                                                        when billtype in ('Solar Meter') and (importkwh-mibillinfo.billread)>1000 then 23 
                                                        when billtype in ('Solar Meter') and (importkwh-mibillinfo.billread) between 0 and 1000  then 25
                                                        when billtype not in ('Solar Meter') and (kwhmi-mibillinfo.billread)<0 then 22
                                                        when billtype not in ('Solar Meter') and (kwhmi-mibillinfo.billread)>1000 then 23 
                                                        when billtype not in ('Solar Meter') and (kwhmi-mibillinfo.billread) between 0 and 1000  then 25 else 5 end) 
            where responselogid=$4;
            END;
        end if;

        --lot 
        select  distnid,isresurvey,serveyorid,locationid,surveydate,a.lotid,nextapproverroleid,appoveddate,a.responsestatusid  from tblresponselogs  a where responselogid=$4 into ipdoc;
 
        if ipdoc.nextapproverroleid in(68) and ipdoc.responsestatusid=25 then 
            BEGIN
                select * into lotidi from getlotid(ipdoc.locationid::int4,ipdoc.nextapproverroleid,to_date(ipdoc.appoveddate,'dd/MM/yyyy'),$3);
                update tblresponselogs set lotid=lotidi,rejected=0 where responselogid=$4; 
            END;
        end if;

    END;
    end if;

     
  ---if metr null then projectmanager
   select value into meter
   from tblresponselogs
   cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
   where responselogid=$4 and items.categorypropertyallocationid in (702);

   if meter=''or meter is null and nextapproleid=11 and respstatusid<>1 then 
        update tblresponselogs set nextapproverroleid=67  where responselogid=$4; 
   end if ;

--resurvey
    if nextapproleid=2 and respstatusid=26 then
        BEGIN

        if not exists(select distid from tblresurvey where distid=distrid and responseid=$4 and resurvey=0) then
        begin
         	select max(surveydetailsid) into maxsdid from tblsurveydetaails where responselogid=$4;
        	update tblresponselogs set resurvey=1 where responselogid=$4;

        	INSERT INTO tblresurvey (distid, olid, resurvey, responseid) 
        	select distrid,(select distinct officelocationallocationid from tbldistributionnodelocationallocation 
        	where distributionnodeid=distrid ), 0, $4 ;
	
        	INSERT INTO tblresurveydetaails (distributionnodeid, olid, surveyid, surveydate, surveytime, walkalongseries, surveytype, latitude, longitude, surveyorname, responselogid, resurveyid) 
        	select distributionnodeid,olid,surveyid,surveydate,surveytime,walkalongseries,surveytype,latitude,longitude,surveyorname,responselogid ,(select max(a.resurveyid) from tblresurvey a where responseid=$4)
        	from tblsurveydetaails where responselogid=$4 and surveydetailsid=maxsdid returning resurveydetailsid into resid;
	
            INSERT INTO tblresurveyproperties (resurveydetailsid, catogorypropertyallocationid,orderid) 
            select resid,categorypropertyallocationid,0
            from tblresponselogs b
            cross join lateral jsonb_to_recordset(b.response::jsonb->'propertiesBean') as items (categorypropertyallocationid int4,value text) 
            where activityid=73 and responsedate is not null and response<>'' and value<>'' and  responselogid=$4 ;

            INSERT INTO tblresurveypropertyvaluedetails (resurveypropertyid, value,valueid)
            with mctea as (
            select  distinct value,responselogid,categorypropertyallocationid,"valueId" valueid
                          from tblresponselogs b
                          cross join lateral jsonb_to_recordset(b.response::jsonb->'propertiesBean') as items (categorypropertyallocationid int4,value text,"valueId" int4) 
                          where activityid=73 and responsedate is not null and response<>'' and value<>'' and responselogid=$4 and value<>'{"mobile_number":null,"validated":null}'
            )
            ,cteas as (
            select resurveypropertyid,catogorypropertyallocationid,s.responselogid from tblresurveydetaails s
            inner join tblresurveyproperties p on p.resurveydetailsid=s.resurveydetailsid
            )
            select resurveypropertyid,value,valueid from mctea a
            inner join cteas b on b.catogorypropertyallocationid=a.categorypropertyallocationid and a.responselogid=b.responselogid;
            
        end;
		end if;

	END;
	end if;

    --OCR data implementation
    SELECT
    MAX(sub_value) FILTER (WHERE sub_cpa = 1407) AS "1407",
    MAX(sub_value) FILTER (WHERE sub_cpa = 1408) AS "1408",
    MAX(sub_value) FILTER (WHERE sub_cpa = 1411) AS "1411",
    MAX(sub_value) FILTER (WHERE sub_cpa = 1421) AS "1421",
    MAX(sub_value) FILTER (WHERE sub_cpa = 1414) AS "1414",
    MAX(sub_value) FILTER (WHERE sub_cpa = 1418) AS "1418",
    MAX(sub_value) FILTER (WHERE sub_cpa = 1406) AS "1406" into ocrdata
    FROM (
        SELECT 
            (subprop->>'categorypropertyallocationid')::int AS sub_cpa,
            subprop->>'value' AS sub_value
        FROM tblresponselogs r,
             LATERAL jsonb_array_elements(r.response::jsonb->'propertiesBean') AS prop,
             LATERAL jsonb_array_elements(prop->'subPropertyList') AS subprop
        WHERE r.responselogid = $4
          AND (prop->>'categorypropertyallocationid')::int IN (739,741,1107,1108)
    ) AS subquery;

    SELECT 
    MAX(value) FILTER (WHERE categorypropertyallocationid = 677) AS "677",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 1105) AS "1105",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 1106) AS "1106",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 694) AS "billtype",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 25) AS "old_phase",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 1416) AS "1416",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 742) AS "742",
    MAX(value) FILTER (WHERE categorypropertyallocationid = 740) AS "740" into ocr_actualdata
    FROM tblresponselogs r
    CROSS JOIN LATERAL jsonb_to_recordset(r.response::jsonb -> 'propertiesBean')
    AS items(value text, categorypropertyallocationid int)
    WHERE r.responselogid = $4
    AND items.categorypropertyallocationid IN (694,677,1105,1106,25,1416,742,740);


    if ocr_actualdata."billtype" in ('Postpaid') and (uppeR(trim(ocrdata."1421"))=upper(trim(ocr_actualdata."1416")) and trim(ocrdata."1414")::numeric=trim(ocr_actualdata."740")::numeric
           and trim(ocrdata."1418")::numeric=trim(ocr_actualdata."742")::numeric) then
            v_flag :=true;
    elsif ocr_actualdata."billtype" in ('Solar Meter') and upper(trim(ocrdata."1407"))=upper(trim(ocr_actualdata."677")) and trim(ocrdata."1408")::numeric=trim(ocr_actualdata."1105")::numeric
          and trim(ocrdata."1411")::numeric=trim(ocr_actualdata."1106")::numeric then 
            v_flag :=true;
    else 
        v_flag :=false;

    end if;
      
        if respstatusid in (0) and nextapproleid=11 then
            if  v_flag then
                update tblresponselogs set responsestatusid=5, nextapproverroleid=9 where responselogid=$4;
                INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks) VALUES ($4, now(), 16, 5, '⭐ AI Verified : Normal Reading is auto-approved');
                insert into tbl_ocr_verified_data (responselogid, distributionnodeid, responsestatusid, nextapproverroleid)
                select responselogid,distnid,responsestatusid,nextapproverroleid from tblresponselogs where responselogid=$4;
                PERFORM  sp_processresponse_1($1,$2,$3,$4);
                return;
           else
            insert into tbl_ocr_nonverified_data (responselogid, distributionnodeid, responsestatusid, nextapproverroleid)
            select responselogid,distnid,responsestatusid,nextapproverroleid from tblresponselogs where responselogid=$4;

           end if;

       end if;
	
	--report
	delete from etl_cimidata where projectid=$1 and activityid=$3 and distributionnodetypeid=$2 and distributionnodeid=distrid;
	

    WITH MinSD AS (
	SELECT * FROM (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,SurveyDate,SurveyTime
			,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID )) MaxNo
	FROM ( SELECT responselogid,projectid,activityid FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2) and B.activityid in($3)) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0 
		and d.distributionnodeid=distrid
	) s WHERE MaxNo=1
	)
	, Sdata AS (
	SELECT s.ProjectID,s.ResponseLogID,s.DistributionNodeID,OLID,SurveyID,s.SurveyDate,s.SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
	,DistributionNodeTypeID,DistributionNodeName,DistributionNodeCode,Approved,AppovedDate ApprovedDate,Rejected,RejectReason,ReSurveyDone,ResponseStatusID
	,ApproverID,s.ActivityID,NextApproverRoleID,LocationID,nextapproverid,s1.SurveyDate Min_SurveyDate,s1.SurveyTime Min_SurveyTime,contactrid contact_rid FROM (
	SELECT * FROM (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,OLID,SurveyID,SurveyDate,SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
			,s.ActivityID Survey_ActivityID
			,distributionnodename,distributionnodecode,DistributionNodeTypeID,UnderDistributionNodeID 
			,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID desc)) MaxNo
	FROM ( SELECT responselogid,projectid,responsedate,responsetime,approved,rejected,appoveddate,locationid,rejectreason,resurveydone,responsestatusid
			,approverid,nextapproverid,activityid,nextapproverroleid,contact_rid contactrid	FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2)   and B.activityid in($3) and distnid=distrid ) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0
	) s WHERE MaxNo=1 ) s
	LEFT JOIN MinSD s1 ON s1.DistributionNodeID=s.DistributionNodeID AND s1.activityid=s.activityid AND s1.projectid=s.projectid
	)
    ,crosstabcte as (
    select *  from crosstab (
	'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
	cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
	where responselogid='||$4 ||' and activityid='||$3||' and nodetype='||$2||' and items.categorypropertyallocationid in (1071,1072,1073,1074,1075,1083,1084,1085,1086,1087,1088,1090,1092,1094,1096,1097,1098,1099,1100,1101,1102,1103,1105,1106,1109,1110,1111,1112,654,655,656,657,658,659,660,676,677,678,679,680,681,682,683,684,685,686,687,688,689,690,691,692,693,694,695,697,698,699,700,701,702,703,704,705,706,707,708,709,710,711,712,713,714,715,716,717,718,719,720,721,722,725,726,727,728,729,730,731,732,733,734,735,736,737,738,740,742,743,744,745,746,747,748,749,750,755,757,
    768,769,771,777,1366,1368)
    ',

	'select distinct ''_''|| categorypropertyallocationid 
	from tblcategorypropertyallocation q  
	right join tblproperties r on r.propertyid=q.propertyid where 
	categorypropertyallocationid in(1071,1072,1073,1074,1075,1083,1084,1085,1086,1087,1088,1090,1092,1094,1096,1097,1098,1099,1100,1101,1102,1103,1105,1106,1109,1110,1111,1112,654,655,656,657,658,659,660,676,677,678,679,680,681,682,683,684,685,686,687,688,689,690,691,692,693,694,695,697,698,699,700,701,702,703,704,705,706,707,708,709,710,711,712,713,714,715,716,717,718,719,720,721,722,725,726,727,728,729,730,731,732,733,734,735,736,737,738,740,742,743,744,745,746,747,748,749,750,755,757,
    768,769,771,777,1366,1368)
    ORDER  BY ''_''|| categorypropertyallocationid '
	 ) 
    as newtable (
    responselogid integer,
     _1071 text,_1072 text,_1073 text,_1074 text,_1075 text,_1083 text,_1084 text,_1085 text,_1086 text,_1087 text,_1088 text,_1090 text,_1092 text,_1094 text,_1096 text,_1097 text,_1098 text,_1099 text,_1100 text,_1101 text,_1102 text,_1103 text,_1105 text,_1106 text,_1109 text,_1110 text,_1111 text,_1112 text,_1366 text,_1368 text,_654 text,_655 text,_656 text,_657 text,_658 text,_659 text,_660 text,_676 text,_677 text,_678 text,_679 text,_680 text,_681 text,_682 text,_683 text,_684 text,_685 text,_686 text,_687 text,_688 text,_689 text,_690 text,_691 text,_692 text,_693 text,_694 text,_695 text,_697 text,_698 text,_699 text,_700 text,_701 text,_702 text,_703 text,_704 text,_705 text,_706 text,_707 text,_708 text,_709 text,_710 text,_711 text,_712 text,_713 text,_714 text,_715 text,_716 text,_717 text,_718 text,_719 text,_720 text,_721 text,_722 text,_725 text,_726 text,_727 text,_728 text,_729 text,_730 text,_731 text,_732 text,_733 text,_734 text,_735 text,_736 text,_737 text,_738 text,_740 text,_742 text,_743 text,_744 text,_745 text,_746 text,_747 text,_748 text,_749 text,_750 text,_755 text,_757 text,_768 text,_769 text,
    _771 text,_777 text)
    )insert into etl_cimidata
    select projectid, a.responselogid, distributionnodeid, olid, surveyid, surveydate, surveytime, surveytype, cycles, latitude, longitude, distributionnodetypeid, distributionnodename, distributionnodecode, approved, approveddate, rejected, rejectreason, resurveydone, responsestatusid, approverid, activityid, nextapproverroleid, locationid, nextapproverid, min_surveydate, min_surveytime ,
    _1071,_1072,_1073,_1074,_1075,_1083,_1084,_1085,_1086,_1087,_1088,_1090,_1092,_1094,_1096,_1097,_1098,_1099,_1100,_1101,_1102,_1103,_1105,_1106,_1109,_1110,_1111,_1112,_654,_655,_656,_657,_658,_659,_660,_676,_677,_678,_679,_680,_681,_682,_683,_684,_685,_686,_687,_688,_689,_690,_691,_692,_693,_694,_695,_697,_698,_699,_700,_701,_702,_703,_704,_705,_706,_707,_708,_709,_710,_711,_712,_713,_714,_715,_716,_717,_718,_719,_720,_721,_722,_725,_726,_727,_728,_729,_730,_731,_732,_733,_734,_735,_736,_737,_738,_740,_742,_743,_744,_745,_746,_747,_748,_749,_750,_755,_757,_768,
    _769,_771,_777,contact_rid,_1366,_1368
    from sdata a
	left join crosstabcte b on a.responselogid=b.responselogid;
	
	
	if respstatusid in(1) then PERFORM push_dist_udf_master($4,$1); 
	end if;
	update tblresponselogs set updated=false where responselogid=$4;
    insert into etl_pushresponselogFORetl(responselogid,activityid,etlupdated) select $4,$3,false ;
	INSERT INTO etl_pushresponselogforetl_dashboard (responselogid, activityid, etlupdated) select $4,$3,false ;
    insert into etl_pushslnoforetl (slno,etlupdated) select _702::jsonb->>'meter_Id'  ,false from etl_cimidata where responselogid=$4 ;
	PERFORM push_dist_udf_master($4,$1);

    if respstatusid in(0) and nextapproleid in(11) then  
            insert into tp_meterinstall_searialno_push_logs (searialno,activityid,rl_rid,updated) 
            select _702::jsonb->>'meter_Id' ,$3,$4,false from etl_cimidata a
            where a.responselogid=$4  and   _702 is not null ;
    end if;
    	
END;

/*elsif $3=73 then

BEGIN

     select value into scanbarstckrhlder
   from tblresponselogs
   cross join lateral jsonb_to_recordset(response::jsonb -> 'propertiesBean') as items( value text,categorypropertyallocationid int)
   where responselogid=$4 and items.categorypropertyallocationid in (700);

    if scanbarstckrhlder is null and nextapproleid=0 and respstatusid<>1 then update tblresponselogs set nextapproverroleid=31  where responselogid=$4; 
    end if ;
	  

	delete from etl_cimidata where projectid=$1 and activityid=$3 and distributionnodetypeid=$2 and distributionnodeid=distrid;
	

    WITH survey AS (
    select projectid, responselogid, distnid, locationid, serveyorid, surveydate,responsetime,surveytype
    ,latitude,longitude,nodetype,distname,distcode,approved,appoveddate,rejected,rejectreason,resurveydone,responsestatusid,approverid,activityid,nextapproverroleid,nextapproverid
    ,B.contact_rid
    from tblresponselogs B WHERE b.ProjectID IN (9) and B.responselogid=$4 and b.nodetype in ($2) and B.activityid in($3)and B.responsestatusid>=0)

    ,MinSD AS (
	SELECT  responselogid, distnid,surveydate min_surveydate,responsetime min_surveytime FROM (
	SELECT r.*,(Row_Number() over (Partition BY r.surveydate ORDER BY r.surveydate,responselogid asc )) MaxNo
	FROM ( SELECT b.* FROM tblresponselogs B WHERE b.ProjectID IN ($1) and B.responselogid=$4
			and b.nodetype in ($2) and B.activityid in($3) and B.responsestatusid>=0) r) s where MaxNo=1)

    ,crosstabcte as (
    select *  from crosstab (
	'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
	cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
	where responselogid='||$4 ||' and activityid='||$3||' and nodetype='||$2||' and items.categorypropertyallocationid in (676,677,678,679,680,681,682,683,684,685,686,687,688,689,690,691,692,693,694,695,697,698,699,700,701,702,704,705,706,707,708,709,710,711,712,713,714,715,716,717,718,719,720,721,722,725,726,727,728,729,730,731,732,733,734,735,736,737,738,740,742,743,744,745,746,747,748)',

	'select distinct ''_''|| categorypropertyallocationid 
	from tblcategorypropertyallocation q  
	right join tblproperties r on r.propertyid=q.propertyid where categoryid in(1,2,3)
	and categorypropertyallocationid in(676,677,678,679,680,681,682,683,684,685,686,687,688,689,690,691,692,693,694,695,697,698,699,700,701,702,704,705,706,707,708,709,710,711,712,713,714,715,716,717,718,719,720,721,722,725,726,727,728,729,730,731,732,733,734,735,736,737,738,740,742,743,744,745,746,747,748)
		ORDER  BY ''_''|| categorypropertyallocationid '
	 ) 
    as newtable (
    responselogid integer,_676 text,_677 text,_678 text,_679 text,_680 text,_681 text,_682 text,_683 text,_684 text,_685 text,_686 text,_687 text,_688 text,_689 text,_690 text,_691 text,_692 text,_693 text,_694 text,_695 text,_697 text,_698 text,_699 text,_700 text,_701 text,_702 text,_704 text,_705 text,_706 text,_707 text,_708 text,_709 text,_710 text,_711 text,_712 text,_713 text,_714 text,_715 text,_716 text,_717 text,_718 text,_719 text,_720 text,_721 text,_722 text,_725 text,_726 text,_727 text,_728 text,_729 text,_730 text,_731 text,_732 text,_733 text,_734 text,_735 text,_736 text,_737 text,_738 text,_740 text,_742 text,_743 text,_744 text,_745 text,_746 text,_747 text,_748 text)
    )insert into etl_cimidata
    select projectid, a.responselogid,a.distnid distributionnodeid, a.locationid olid, serveyorid surveyid,a.surveydate surveydate,a.responsetime surveytime,a.surveytype,a.latitude,a.longitude,a.nodetype distributionnodetypeid,a.distname distributionnodename,a.distcode distributionnodecode,a.approved, a.appoveddate,a.rejected,a.rejectreason,a.resurveydone,a.responsestatusid,a.approverid,a.activityid,a.nextapproverroleid, nextapproverid, min_surveydate, min_surveytime, 
    _676,_677,_678,_679,_680,_681,_682,_683,_684,_685,_686,_687,_688,_689,_690,_691,_692,_693,_694,_695,_697,_698,_699,_700,_701,_702,_704,_705,_706,_707,_708,_709,_710,_711,_712,_713,_714,_715,_716,_717,_718,_719,_720,_721,_722,_725,_726,_727,_728,_729,_730,_731,_732,_733,_734,_735,_736,_737,_738,_740,_742,_743,_744,_745,_746,_747,_748
    from survey a
    inner join MinSD c on c.responselogid=a.responselogid
    left join crosstabcte b on a.responselogid=b.responselogid;
	
	if respstatusid in(1) then PERFORM push_dist_udf_master($4,$1); 
	end if;
	update tblresponselogs set updated=false where responselogid=$4;
    insert into etl_pushresponselogFORetl(responselogid,activityid,etlupdated) select $4,$3,false ;

	PERFORM push_dist_udf_master($4,$1);

END;*/
            --package 2 insert
 if not exists (select 1 from tbl_distinid_package where distinid=distrid) then 
        INSERT INTO tbl_distinid_package (distinid, activityid, activity_package, updateddatetime) 
        select a.distributionnodeid,a.activityid,b.warehouse_package,now() from  etl_cimidata a join tbl_meterno_package b
        on upper(a._702::jsonb->>'meter_Id')=b.serialno where a.responselogid=$4;

	end if;

elsif $3=45 then
    BEGIN
      
        update tblresponselogs set nextapproverroleid=10 ,nextapproverid=0 where responselogid=$4 and responsestatusid=28 and nextapproverroleid=0;
     /*select value into distid_subs
     from tblresponselogs a2
     cross join lateral jsonb_to_recordset(a2.response::jsonb->'propertiesBean') as items(categorypropertyallocationid int4,value text)  
     where activityid=45 and categorypropertyallocationid=653 and projectid=9 and a2.responselogid=$4;

     select responselogid,distnid,serveyorid,activityid,nodetype from tblresponselogs where activityid=45 and projectid=9 and responselogid=$4 into distid_substation_data;

       if  exists(select d_node_user_seq_id from distribution_nodetype_user_seq_maping where userid=distid_substation_data.serveyorid and dist_nodetype_id=$2 ) then
           begin
 			   if not exists(select distnid from tblresponselogs r where r.distnid=distid_substation_data.distnid and r.activityid=45 and r.resurvey=1 and r.projectid=9) then
               begin
               update distribution_nodetype_user_seq_maping set dummy_code=dummy_code+1,updated_datetime=now(),current_distid=distid_substation_data.distnid,current_rlid=distid_substation_data.responselogid 
                ,dist_nodetype_id=distid_substation_data.nodetype,userid=distid_substation_data.serveyorid,activityid=distid_substation_data.activityid,distnid=distid_subs::int4
                where userid=distid_substation_data.serveyorid and current_distid<>distid_substation_data.distnid and current_rlid<>$4 and dist_nodetype_id=$2 and $4>current_rlid; 
                       
                       if not exists(select dummy_code from distribution_nodetype_user_seq_maping_history where current_rlid=$4 and userid=distid_substation_data.serveyorid) then
                       begin
                            insert into distribution_nodetype_user_seq_maping_history (dist_nodetype_id,userid,activityid,dummy_code,updated_datetime,parent_distnid,current_distid,current_rlid) 
                          select $2,distid_substation_data.serveyorid,distid_substation_data.activityid,(select dummy_code from distribution_nodetype_user_seq_maping where current_distid=distid_substation_data.distnid)
                            ,now(),distid_subs::int4,distid_substation_data.distnid,distid_substation_data.responselogid;
                       end;
                       end if;
			   end;
               end if;
           end;
       else
            begin

              insert into distribution_nodetype_user_seq_maping (dist_nodetype_id,userid,activityid,dummy_code,updated_datetime,distnid,current_distid,current_rlid) 
              select $2,distid_substation_data.serveyorid,distid_substation_data.activityid,1,now(),distid_subs::int4,distid_substation_data.distnid,distid_substation_data.responselogid;

              insert into distribution_nodetype_user_seq_maping_history (dist_nodetype_id,userid,activityid,dummy_code,updated_datetime,parent_distnid,current_distid,current_rlid) 
              select $2,distid_substation_data.serveyorid,distid_substation_data.activityid,1,now(),distid_subs::int4,distid_substation_data.distnid,distid_substation_data.responselogid;
            end;
       end if; */
	

	delete from etl_feeder_cidata where projectid=$1 and activityid=$3 and distributionnodetypeid=$2 and distributionnodeid=distrid;
	
	WITH survey AS (
    select projectid, responselogid, distnid, locationid, serveyorid, surveydate,responsetime,surveytype
    ,latitude,longitude,nodetype,distname,distcode,approved,appoveddate,rejected,rejectreason,resurveydone,responsestatusid,approverid,activityid,nextapproverroleid,nextapproverid
    ,B.contact_rid
    from tblresponselogs B WHERE b.ProjectID IN ($1) and B.responselogid=$4 and b.nodetype in ($2) and B.activityid in($3)and B.responsestatusid>=0)

    ,MinSD AS (
	SELECT  responselogid, distnid,surveydate min_surveydate,responsetime min_surveytime FROM (
	SELECT r.*,(Row_Number() over (Partition BY r.surveydate ORDER BY r.surveydate,responselogid asc )) MaxNo
	FROM ( SELECT b.* FROM tblresponselogs B WHERE b.ProjectID IN ($1) and B.responselogid=$4
			and b.nodetype in ($2) and B.activityid in($3) and B.responsestatusid>=0) r) s where MaxNo=1)

    ,crosstabcte as (
    select *  from crosstab (
	'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
	cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
	where responselogid='||$4 ||' and activityid='||$3||' and nodetype='||$2||' and items.categorypropertyallocationid in (581,582,583,584,585,586,587,588,589,590,591,592,593,594,595,596,597,598,599,600,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618,619,620,621,622,645,646,647,648,649,650,651,653,671)',

	'select distinct ''_''|| categorypropertyallocationid 
	from tblcategorypropertyallocation q  
	right join tblproperties r on r.propertyid=q.propertyid where categoryid in(79,80)
	and categorypropertyallocationid in(581,582,583,584,585,586,587,588,589,590,591,592,593,594,595,596,597,598,599,600,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618,619,620,621,622,645,646,647,648,649,650,651,653,671)
		ORDER  BY ''_''|| categorypropertyallocationid '
	 ) 
    as newtable (
    responselogid integer,_581 text,_582 text,_583 text,_584 text,_585 text,_586 text,_587 text,_588 text,_589 text,_590 text,_591 text,_592 text,_593 text,_594 text,_595 text,_596 text,_597 text,_598 text,_599 text,_600 text,_601 text,_602 text,_603 text,_604 text,_605 text,_606 text,_607 text,_608 text,_609 text,_610 text,_611 text,_612 text,_613 text,_614 text,_615 text,_616 text,_617 text,_618 text,_619 text,_620 text,_621 text,_622 text,_645 text,_646 text,_647 text,_648 text,_649 text,_650 text,_651 text,_653 text,_671 text)
    )insert into etl_feeder_cidata
    select projectid, a.responselogid,a.distnid distributionnodeid, a.locationid olid, serveyorid surveyid,a.surveydate surveydate,a.responsetime surveytime,a.surveytype,a.latitude,a.longitude,a.nodetype distributionnodetypeid,a.distname distributionnodename,a.distcode distributionnodecode,a.approved, a.appoveddate,a.rejected,a.rejectreason,a.resurveydone,a.responsestatusid,a.approverid,a.activityid,a.nextapproverroleid, nextapproverid, min_surveydate, min_surveytime, 
    _581,_582,_583,_584,_585,_586,_587,_588,_589,_590,_591,_592,_593,_594,_595,_596,_597,_598,_599,_600,_601,_602,_603,_604,_605,_606,_607,_608,_609,_610,_611,_612,_613,_614,_615,_616,_617,_618,_619,_620,_621,_622,_645,_646,_647,_648,_649,_650,_651,_653,_671,contact_rid
    from survey a
    inner join MinSD c on c.responselogid=a.responselogid
    left join crosstabcte b on a.responselogid=b.responselogid;
	
    if respstatusid in(1) then PERFORM push_dist_udf_master($4,$1); end if;
	update tblresponselogs set updated=false where responselogid=$4;
    insert into etl_pushresponselogFORetl(responselogid,activityid,etlupdated) select $4,$3,false ;

	PERFORM push_dist_udf_master($4,$1);
	
end;

elsif $3=46 then
    BEGIN
         
        update tblresponselogs set nextapproverroleid=10 ,nextapproverid=0 where responselogid=$4 and responsestatusid=28 and nextapproverroleid=0;
        delete from etl_feeder_midata where projectid=$1 and activityid=$3 and distributionnodetypeid=$2 and distributionnodeid=distrid;

        WITH survey AS (
        select projectid, responselogid, distnid, locationid, serveyorid, surveydate,responsetime,surveytype
        ,latitude,longitude,nodetype,distname,distcode,approved,appoveddate,rejected,rejectreason,resurveydone
        ,responsestatusid,approverid,activityid,nextapproverroleid,nextapproverid,B.contact_rid
        from tblresponselogs B 
        WHERE b.ProjectID IN ($1) and B.responselogid=$4 and b.nodetype in ($2) and B.activityid in($3) and B.responsestatusid>=0
        )

        ,MinSD AS (
            SELECT  responselogid, distnid,surveydate min_surveydate,responsetime min_surveytime FROM (
            SELECT r.*,(Row_Number() over (Partition BY r.surveydate ORDER BY r.surveydate,responselogid asc )) MaxNo
            FROM ( SELECT b.* FROM tblresponselogs B WHERE b.ProjectID IN ($1) and B.responselogid=$4
                    and b.nodetype in ($2) and B.activityid in($3) and B.responsestatusid>=0) r
        ) s where MaxNo=1	
        )

        ,crosstabcte as (
        select *  from crosstab (
            'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
            cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
            where activityid='||$3||' and nodetype='||$2||' and items.categorypropertyallocationid in (581,582,583,584,585,586,587,588,589,590,591,592,593,594,595,596,597,598,599,600,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618,619,620,621,622,623,624,625,626,627,628,629,630,631,632,633,634,635,636,637,645,646,647,648,649,650,651,653,671)',

            'select distinct ''_''|| categorypropertyallocationid 
            from tblcategorypropertyallocation q  
            right join tblproperties r on r.propertyid=q.propertyid where categoryid in(79,80,81)
            and categorypropertyallocationid in(581,582,583,584,585,586,587,588,589,590,591,592,593,594,595,596,597,598,599,600,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618,619,620,621,622,623,624,625,626,627,628,629,630,631,632,633,634,635,636,637,645,646,647,648,649,650,651,653,671)
                ORDER  BY ''_''|| categorypropertyallocationid '
             ) 
        as newtable (
        responselogid integer,_581 text,_582 text,_583 text,_584 text,_585 text,_586 text,_587 text,_588 text,_589 text
            ,_590 text,_591 text,_592 text,_593 text,_594 text,_595 text,_596 text,_597 text,_598 text,_599 text,_600 text
            ,_601 text,_602 text,_603 text,_604 text,_605 text,_606 text,_607 text,_608 text,_609 text,_610 text,_611 text
            ,_612 text,_613 text,_614 text,_615 text,_616 text,_617 text,_618 text,_619 text,_620 text,_621 text,_622 text
            ,_623 text,_624 text,_625 text,_626 text,_627 text,_628 text,_629 text,_630 text,_631 text,_632 text,_633 text
            ,_634 text,_635 text,_636 text,_637 text,_645 text,_646 text,_647 text,_648 text,_649 text,_650 text,_651 text
            ,_653 text,_671 text)
        )insert into etl_feeder_midata

        select projectid, a.responselogid,a.distnid distributionnodeid, a.locationid olid, serveyorid surveyid,a.surveydate surveydate,a.responsetime surveytime,a.surveytype,a.latitude,a.longitude,a.nodetype distributionnodetypeid,a.distname distributionnodename,a.distcode distributionnodecode,a.approved, a.appoveddate,a.rejected,a.rejectreason,a.resurveydone,a.responsestatusid,a.approverid,a.activityid,a.nextapproverroleid, nextapproverid, min_surveydate, min_surveytime, 
        _581,_582,_583,_584,_585,_586,_587,_588,_589,_590,_591,_592,_593,_594,_595,_596,_597,_598,_599,_600,_601,_602,_603
        ,_604,_605,_606,_607,_608,_609,_610,_611,_612,_613,_614,_615,_616,_617,_618,_619,_620,_621,_622,_623,_624,_625,_626
        ,_627,_628,_629,_630,_631,_632,_633,_634,_635,_636,_637,_645,_646,_647,_648,_649,_650,_651,_653,_671,contact_rid
        from survey a
        inner join MinSD c on c.responselogid=a.responselogid
        left join crosstabcte b on a.responselogid=b.responselogid;

        if respstatusid in(1) then PERFORM push_dist_udf_master($4,$1); end if;
        update tblresponselogs set updated=false where responselogid=$4;
        insert into etl_pushresponselogFORetl(responselogid,activityid,etlupdated) select $4,$3,false ;
        insert into etl_pushslnoforetl (slno,etlupdated) select _624::jsonb->>'meter_Id' ,false from etl_feeder_midata where responselogid=$4;

    END;	
elsif $3=78 then
    BEGIN
        delete from etl_pdi where responselogid=$4;

        with pdicte as (
        select *  from crosstab (
        'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
        cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
        where responselogid='||$4||' and nodetype='|| $2||' and items.categorypropertyallocationid in (1000,999,990,1009,1010,1001)',

        'select distinct ''_''|| categorypropertyallocationid
        from tblcategorypropertyallocation q   
        where categorypropertyallocationid in(1000,999,990,1009,1010,1001)
            ORDER  BY ''_''|| categorypropertyallocationid '
         )
         as newtable (
          responselogid integer,_1000 text,_1001 text,_1009 text,_1010 text,_990 text,_999 text
         ))
         insert into etl_pdi
         select a.*,b.serveyorid,b.contact_rid,b.responsestatusid,b.nextapproverroleid,b.locationid,b.distnid,b.distcode
         from pdicte a
         left join tblresponselogs b on a.responselogid=b.responselogid
         where a.responselogid=$4;

    END;

elsif $3=3 then
    BEGIN
    if respstatusid in(1) then PERFORM push_dist_udf_master($4,$1); end if;
	
    --sm50 bucket
	 select roleid into holdroleid from tblprojectuserallocation where userid in(Select nextapproverid from tblresponselogs where responselogid=$4 ) and projectid=13;
        if $3=3 and (nextapproleid=9 or holdroleid=9) then--and respstatusid<>7 and not exists(select 1 from tblsm_50 where distributionnodeid=distrid and valid=true)  then
               --perform  fn_update_sm50status(distrid,$4);
        elsif $3=3 and (nextapproleid=9 or holdroleid=9) then--and respstatusid<>7  and exists(select 1 from tblsm_50 where distributionnodeid=distrid and valid=true) then
            update tblresponselogs set respstatusid=5 where responselogid=$4;
        end if;
       
   delete from etl_nscdata where distributionnodeid=distrid and projectid=$1 and activityid=$3 and distributionnodetypeid=$2;

    --Ping test
    if resurveyid = 1 AND respstatusid = 3 THEN
        DELETE FROM tblresurveypropertyvaluedetails 
        WHERE resurveypropertyvaluedetailsid IN (
            SELECT DISTINCT d.resurveypropertyvaluedetailsid
            FROM tblresurvey a
            LEFT JOIN tblresurveydetaails b 
                   ON a.resurveyid = b.resurveyid
            INNER JOIN tblresurveyproperties c 
                   ON b.resurveydetailsid = c.resurveydetailsid
            INNER JOIN tblresurveypropertyvaluedetails d 
                   ON c.resurveypropertyid = d.resurveypropertyid
            WHERE a.responseid = $4
              AND c.catogorypropertyallocationid IN (1427, 1428, 1429)
        );
    end if;
    ----ping test-- 

   WITH MinSD AS (
	SELECT * FROM (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,SurveyDate,SurveyTime
			,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID )) MaxNo
	FROM ( SELECT responselogid,projectid,activityid FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2) and B.activityid in($3)) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0 
		and d.distributionnodeid=distrid
	) s WHERE MaxNo=1
	)
	, Sdata AS (
	SELECT s.ProjectID,s.ResponseLogID,s.DistributionNodeID,OLID,SurveyID,s.SurveyDate,s.SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
	,DistributionNodeTypeID,DistributionNodeName,DistributionNodeCode,Approved,AppovedDate ApprovedDate,Rejected,RejectReason,ReSurveyDone,ResponseStatusID
	,ApproverID,s.ActivityID,NextApproverRoleID,LocationID,nextapproverid,s1.SurveyDate Min_SurveyDate,s1.SurveyTime Min_SurveyTime,contactrid contactrid FROM (
	SELECT * FROM (
	SELECT r.* ,SurveyDetailsID,s.DistributionNodeID,OLID,SurveyID,SurveyDate,SurveyTime,SurveyType,Cycles,s.Latitude,s.Longitude
			,s.ActivityID Survey_ActivityID
			,distributionnodename,distributionnodecode,DistributionNodeTypeID,UnderDistributionNodeID 
			,(Row_Number() over (Partition BY s.distributionnodeid,s.activityid ORDER BY s.distributionnodeid,s.activityid,SurveyDetailsID desc)) MaxNo
	FROM ( SELECT responselogid,projectid,responsedate,responsetime,approved,rejected,appoveddate,locationid,rejectreason,resurveydone,responsestatusid
			,approverid,nextapproverid,activityid,nextapproverroleid,contact_rid contactrid	FROM tblresponselogs B WHERE b.ProjectID IN ($1)
			and b.nodetype in ($2)   and B.activityid in($3) and distnid=distrid ) r
	INNER JOIN tblsurveydetaails s ON s.responselogid=r.responselogid
	INNER JOIN tbldistributionnodes  d ON d.distributionnodeid=s.distributionnodeid AND COALESCE(deleted,0)=0
	) s WHERE MaxNo=1 ) s
	LEFT JOIN MinSD s1 ON s1.DistributionNodeID=s.DistributionNodeID AND s1.activityid=s.activityid AND s1.projectid=s.projectid
	)
	,crosstabcte as (
	select *  from crosstab (
	'select responselogid,''_''||categorypropertyallocationid,value::text val from tblresponselogs
	cross join lateral jsonb_to_recordset(response::jsonb -> ''propertiesBean'') as items( value text,categorypropertyallocationid int)
	where responselogid='||$4||' and nodetype='|| $2 ||' and items.categorypropertyallocationid in (116,117,118,119,120,121,122,123,124,125,126,127,129,130,131,132,133,134,135,136,137,138,139,140,141,143,145,163,166,170,171,172,173,174,1138,1139,1140,1141,1142,1143,1144,1146,1148,1149,1150,1151,1152,1153,1154,1155,1158,1159,1160,1162,1163,1164,
     1165,1166,1167,1168,1169,1170,1171,1172,1173,1176,1177)',

	'select distinct ''_''|| categorypropertyallocationid 
	from tblcategorypropertyallocation q  
	right join tblproperties r on r.propertyid=q.propertyid where --categoryid in(1,2,3,90)
	 categorypropertyallocationid in(116,117,118,119,120,121,122,123,124,125,126,127,129,130,131,132,133,134,135,136,137,138,139,140,141,143,145,163,166,170,171,172,173,174,1138,1139,1140,1141,1142,1143,1144,1146,1148,1149,1150,1151,1152,1153,1154,1155,1158,1159,1160,1162,1163,1164,
      1165,1166,1167,1168,1169,1170,1171,1172,1173,1176,1177)
		ORDER  BY ''_''|| categorypropertyallocationid '
	 )
	 as newtable (
	   responselogid integer,_1138 text,_1139 text,_1140 text,_1141 text,_1142 text,_1143 text,_1144 text,_1146 text,_1148 text,_1149 text,_1150 text,_1151 text,_1152 text,_1153 text,_1154 text,_1155 text,_1158 text,_1159 text,_116 text,_1160 text,_1162 text,_1163 text,_1164 text,_1165 text,_1166 text,_1167 text,_1168 text,_1169 text,_117 text,_1170 text,_1171 text,_1172 text,_1173 text,_1176 text,_1177 text,_118 text,_119 text,_120 text,_121 text,_122 text,_123 text,_124 text,_125 text,_126 text,_127 text,_129 text,_130 text,_131 text,_132 text,_133 text,_134 text,_135 text,_136 text,_137 text,_138 text,_139 text,_140 text,_141 text,_143 text,_145 text,_163 text,_166 text
       ,_170 text,_171 text,_172 text,_173 text,_174 text
	 )
	)insert into etl_nscdata
	select projectid, a.responselogid, distributionnodeid, olid, surveyid, surveydate, surveytime, surveytype, cycles, latitude, longitude, distributionnodetypeid, distributionnodename, distributionnodecode, approved, approveddate, rejected, rejectreason, resurveydone, responsestatusid, approverid, activityid, nextapproverroleid, locationid, nextapproverid, min_surveydate, min_surveytime 
   ,contactrid,_1138,_1139,_1140,_1141,_1142,_1143,_1144,_1146,_1148,_1149,_1150,_1151,_1152,_1153,_1154,_1155,_1158,_1159,_116,_1160,_1162,_1163,_1164,_1165,_1166,_1167,_1168,_1169,_117,_1170,_1171,_1172,_1173,_1176,_1177,_118,_119,_120,_121,_122,_123,_124,_125,_126,_127,_129,_130,_131,_132,_133,_134,_135,_136,_137,_138,_139,_140,_141,_143,_145,_163,_166,_170,_171,_172,_173,_174
	from sdata a
	left join crosstabcte b on a.responselogid=b.responselogid;
	
	update tblresponselogs set updated=false where responselogid=$4;
    insert into etl_pushresponselogFORetl(responselogid,activityid,etlupdated) select $4,$3,false ;
    INSERT INTO etl_pushresponselogforetl_dashboard (responselogid, activityid, etlupdated) select $4,$3,false ;
    insert into etl_pushslnoforetl (slno,etlupdated) select (case when coalesce(_57,'null') ='null' then _58 else _57::jsonb->>'meter_Id' end) ,false from etl_midata where responselogid=$4;

	PERFORM push_dist_udf($4,$1);

    if respstatusid in(0) and nextapproleid in(11,67) then  
                insert into tp_meterinstall_searialno_push_logs (searialno,activityid,rl_rid,updated) 
                select _1144::jsonb->>'meter_Id' ,$3,$4,false from etl_nscdata a
                where a.responselogid=$4  and   _1144 is not null ;
            end if;

--package 2 insert
 if not exists (select 1 from tbl_distinid_package where distinid=distrid) then 
        INSERT INTO tbl_distinid_package (distinid, activityid, activity_package, updateddatetime)
        select a.distributionnodeid,a.activityid,b.warehouse_package,now() from  etl_nscdata a join tbl_meterno_package b
        on upper(a._1144::jsonb->>'meter_Id')=b.serialno where a.responselogid=$4;
end if;


    END;

end if;

end;

end if;
   
end;
$BODY$
  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER