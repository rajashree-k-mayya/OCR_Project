CREATE OR REPLACE FUNCTION fn_ocr_autoapproval(in distributionnodeid int4, in reslogid int4)
  RETURNS void AS 
$BODY$ 
DECLARE
    v_responselogid text; 
BEGIN

	insert into assignmentpreelogs(fntext,created) select 'fn_ocr_autoapproval('||$1::text ||','||$2::text||')',now();

    with verified_data as(
    select m.responselogid,m.nextapproverroleid from etl_midata m
    join tbl_ocr_verified_data ov on ov.responselogid=m.responselogid
    --join tblsm_50 s on s.distributionnodeid=m.distributionnodeid and s.valid=true
    where  m.responselogid=$2 and  m.responsestatusid=25 and m.nextapproverroleid=9
    union 
    select m.responselogid,m.nextapproverroleid from etl_cimidata m
    join tbl_ocr_verified_data ov on ov.responselogid=m.responselogid
    --join tblsm_50 s on s.distributionnodeid=m.distributionnodeid and s.valid=true
    where  m.responselogid=$2 and m.responsestatusid=25 and m.nextapproverroleid=9
    )
    select string_agg(v.responselogid::text,',') into v_responselogid
    from verified_data v;


        update tblresponselogs r set responsestatusid=1, nextapproverroleid=0, nextapproverid=0,approverid=19,approved=1,appoveddate=CURRENT_DATE,resurvey=0,updated = true 
        where responselogid::text in (select regexp_split_to_table(v_responselogid,','));

        INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks) 
        select r.responselogid,now(),17,6,'⭐ AI Verified : Normal Reading is auto-approved' from tblresponselogs r where responselogid::text in(select regexp_split_to_table(v_responselogid,','));

        INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks) 
        select r.responselogid,now(),18,28,'⭐ AI Verified : Normal Reading is auto-approved' from tblresponselogs r where responselogid::text in(select regexp_split_to_table(v_responselogid,','));

        INSERT INTO tblapprovallogs (responselogid, logtime, userid, statusid, remarks) 
        select r.responselogid,now(),19,1,'⭐ AI Verified : Normal Reading is auto-approved' from tblresponselogs r where responselogid::text in(select regexp_split_to_table(v_responselogid,','));

        perform b.* FROM tblresponselogs 
        CROSS JOIN LATERAL sp_processresponse_1(13, 1,  tblresponselogs.activityid, tblresponselogs.responselogid) as b
        where responselogid::text in(select regexp_split_to_table(v_responselogid,','));


END;
$BODY$
  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER