CREATE OR REPLACE FUNCTION getstatusinfo(in selectedactvityid int4, in login_user_roleid int4, in resplogid int4, in statusids int4, in inputjson varchar, in lotid int4)
  RETURNS TABLE (statusid int4, description varchar, reasonrequired bool, nextapproval bool, fileprocess bool, allowdraft bool, isdefault int2) AS 
$BODY$

declare cnbval varchar;

begin
insert into assignmentpreelogs(fntext,created) 
select 'getstatusinfo-2 - '||$1::text||'/'||$2::text||'/'||$3::text||'/'||$4::text||'/'||$5::text ||'/'||$6::text  ,now();


if $3<>0 and $1=71 and $4=15 then 
    BEGIN 
    return query(select b.statusid,b.description,b.reasonrequired,b.nextapproval
                 ,(case when b.statusid=1 and a.dcg_rid=71 then true else b.fileprocess end),b.allowdraft,a.is_default
    from  tblstatus b 
    inner join se_dcg_approval_role_status a on b.statusid=a.status_rid  and a.dcg_rid=$1
    inner join se_dcg_approval_index c on c.dcg_ai_rid=a.dcg_ai_rid			 
    where a.role_rid=$2 and b.statusid<>5);

    END;
/*
--OCR Freezing
elsif $1=72 and $2 in (9) and $4 not in (25)   then 
 
        return query(select b.statusid,b.description,b.reasonrequired,b.nextapproval,b.fileprocess ,b.allowdraft,a.is_default
    from  tblstatus b 
    inner join se_dcg_approval_role_status a on b.statusid=a.status_rid  and a.dcg_rid=$1
    inner join se_dcg_approval_index c on c.dcg_ai_rid=a.dcg_ai_rid			 
    where a.role_rid=$2 and b.statusid in (7,2));
        return query(
    select -1,''::varchar,'0'::bool,'0'::bool,'0'::bool,'0'::bool,0::int2

    );
*/
/*
elsif $1=72 and $2 in (11) and $4  in (57,7)   then 
     return query(select b.statusid,b.description,b.reasonrequired,b.nextapproval,b.fileprocess ,b.allowdraft,a.is_default
        from  tblstatus b 
        inner join se_dcg_approval_role_status a on b.statusid=a.status_rid  and a.dcg_rid=$1
        inner join se_dcg_approval_index c on c.dcg_ai_rid=a.dcg_ai_rid			 
        where a.role_rid=$2 and  b.statusid not in (5));
            
*/
----------ping 
--elsif $1=72 and $2 in (9) and $4  in (54) then 
--  return query(
--    select -1,''::varchar,'0'::bool,'0'::bool,'0'::bool,'0'::bool,0::int2
--);
----------ping


elsif   $1 in(72,73,3) and $4 in(50,51,24) then 
    return query(
    select -1,''::varchar,'0'::bool,'0'::bool,'0'::bool,'0'::bool,0::int2
    );

elsif   $1 in(11) and $4 in(3) then 
    return query(
    select -1,''::varchar,'0'::bool,'0'::bool,'0'::bool,'0'::bool,0::int2
    );
 
elsif $1 in(10,11) and $3<>0 and $4 in(10) then 
    BEGIN 
        return query(select b.statusid,b.description,b.reasonrequired,b.nextapproval,b.fileprocess ,b.allowdraft,a.is_default
    from  tblstatus b 
    inner join se_dcg_approval_role_status a on b.statusid=a.status_rid  and a.dcg_rid=$1
    inner join se_dcg_approval_index c on c.dcg_ai_rid=a.dcg_ai_rid			 
    where a.role_rid=$2 and b.statusid<>1);
    END;
/*
elsif $1 in(10) and $3<>0 and $2 in(30) then 
    BEGIN 
         if ($5 is not null and $5<>'' and $5::varchar like ('[%') and length($5)>5 ) then
            begin
                with tempcte as (select jsonb_array_elements($5::jsonb) as jsonval )
                ,tempcte1 as (select jsonval::json->>'cpaRid' cpaid,jsonval::json->>'valueRid' val from tempcte )
                select val into cnbval from tempcte1 where cpaid='1360' ; 
            end;
            end if;
    if cnbval:: int4 =1171 then
        begin 

            return query(select b.statusid,b.description,b.reasonrequired,b.nextapproval,b.fileprocess ,b.allowdraft,a.is_default
        from  tblstatus b 
        inner join se_dcg_approval_role_status a on b.statusid=a.status_rid  and a.dcg_rid=$1
        inner join se_dcg_approval_index c on c.dcg_ai_rid=a.dcg_ai_rid			 
        where a.role_rid=$2 and  b.statusid in (3,1));
        END;

    elsif cnbval:: int4 =1170 then
        begin
            return query(select b.statusid,b.description,b.reasonrequired,b.nextapproval,b.fileprocess ,b.allowdraft,a.is_default
        from  tblstatus b 
        inner join se_dcg_approval_role_status a on b.statusid=a.status_rid  and a.dcg_rid=$1
        inner join se_dcg_approval_index c on c.dcg_ai_rid=a.dcg_ai_rid			 
        where a.role_rid=$2 and b.statusid<>1 and b.statusid in (3,52));
        END;
end if;
END;
*/
/*
--lab observation

elsif $1 in(10) and $2 in(30) and $3<>0 and $4=0 then 
BEGIN
    if ($5 is not null and $5<>'' and $5::varchar like ('[%') and length($5)>5 ) then
        begin
               with tempcte as (select jsonb_array_elements($5::jsonb) as jsonval )
               ,tempcte1 as (select jsonval::json->>'cpaRid' cpaid,jsonval::json->>'valueRid' val from tempcte )
               select val into cnbval from tempcte1 where cpaid='1360' ; 
        end;
        end if;
        if cnbval:: int4 =1171 then
            begin
            return query(
                select b.statusid,b.description,b.reasonrequired,b.nextapproval
                 ,(case when b.statusid=1 and a.dcg_rid=71 then true else b.fileprocess end),b.allowdraft,a.is_default
                from  tblstatus b 
                inner join se_dcg_approval_role_status a on b.statusid=a.status_rid  and a.dcg_rid=$1
                inner join se_dcg_approval_index c on c.dcg_ai_rid=a.dcg_ai_rid			 
                where a.role_rid=$2 and  b.statusid in (1)
                );
            end;
        elsif cnbval:: int4 =1170 then
            begin
            return query(
                select b.statusid,b.description,b.reasonrequired,b.nextapproval
                 ,(case when b.statusid=1 and a.dcg_rid=71 then true else b.fileprocess end),b.allowdraft,a.is_default
                from  tblstatus b 
                inner join se_dcg_approval_role_status a on b.statusid=a.status_rid  and a.dcg_rid=$1
                inner join se_dcg_approval_index c on c.dcg_ai_rid=a.dcg_ai_rid			 
                where a.role_rid=$2 and b.statusid in (53)
                );
            end;
            end if;
END;
*/
else 
    RETURN Query(select b.statusid,b.description,b.reasonrequired,b.nextapproval
                 ,(case when b.statusid=1 and a.dcg_rid=71 then true else b.fileprocess end),b.allowdraft,a.is_default
    from  tblstatus b 
    inner join se_dcg_approval_role_status a on b.statusid=a.status_rid  and a.dcg_rid=$1
    inner join se_dcg_approval_index c on c.dcg_ai_rid=a.dcg_ai_rid			 
    where a.role_rid=$2);
end if;


end;
$BODY$
  LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER