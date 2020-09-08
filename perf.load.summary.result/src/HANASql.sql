create table SFQA05_TESTH_E.perflogbase (gid NVARCHAR(500),
                                         eid NVARCHAR(500),
                                         rqt DECIMAL(38, 0),
                                         url NVARCHAR(5000),
                                         logdate TIMESTAMP,
                                         testiter INTEGER,
                                         testreqname NVARCHAR(500),
                                         hostname NVARCHAR(500),
                                         testscriptname NVARCHAR(500),
                                         teststep NVARCHAR(500));
                                         
create table SFQA05_TESTH_E.perflognew (gid NVARCHAR(500),
                                         eid NVARCHAR(500),
                                         rqt DECIMAL(38, 0),
                                         url NVARCHAR(5000),
                                         logdate TIMESTAMP,
                                         testiter INTEGER,
                                         testreqname NVARCHAR(500),
                                         hostname NVARCHAR(500),
                                         testscriptname NVARCHAR(500));
insert into SFQA05_TESTH_E.perflogbase(gid, logdate) values ('a6cee313a586f9a34fabc74403ae9b75', TO_TIMESTAMP ('2020-01-14 05:55:02', 'YYYY-MM-DD HH24:MI:SS'));     
insert into SFQA05_TESTH_E.perflogbase(gid, logdate) values ('a6cee313a586f9a34fabc74403ae9b75', TO_TIMESTAMP ('2010-01-11 13:30:00', 'YYYY-MM-DD HH24:MI:SS'));  
                                   
select SCHEMA_NAME,sum(DISK_SIZE) from M_TABLE_PERSISTENCE_STATISTICS where SCHEMA_NAME = 'SFQA05_TESTH_E' group by SCHEMA_NAME
                                           
select * from SFQA05_TESTH_E.perflogbase order by logdate desc;
select count(gid) from SFQA05_TESTH_E.perflogbase;
select * from SFQA05_TESTH_E.perflognew;                                        
delete from SFQA05_TESTH_E.perflogbase;
drop table  SFQA05_TESTH_E.perflogbase;
drop table  SFQA05_TESTH_E.perflognew;

select round((rqt - testiter)/(testiter+1), 2) from SFQA05_TESTH_E.perflogbase;


/* Request level average RQT
  */
select testscriptname,teststep, testreqname , avg(rqt) as avgrqt, (avg(rqt) * (1+ 0.3)) as avgrqtUp30Percent,  min(testiter) as maxtestiter, max(testiter) as maxtestiter, count(teststep) as teststepcount from 
                 (select * from SFQA05_TESTH_E.perflogbase where logdate > TO_TIMESTAMP ('2020-01-15 03:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and logdate < TO_TIMESTAMP ('2020-01-15 06:01:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and hostname like 'mo-8445329fd.lab-rot.saas.sap.corp' )  group by testscriptname, teststep, testreqname order by testscriptname, teststep, testreqname  
                                        


/**Select requests > 30%+ average RQT of base **/
select newResult.testscriptname, newResult.teststep, newResult.testreqname, newResult.gid, newResult.eid, newResult.rqt, summarizdAvgRQTTable.avgrqt, summarizdAvgRQTTable.avgrqtUp30Percent
 from ( select testscriptname,teststep, testreqname, rqt, gid, eid from SFQA05_TESTH_E.perflogbase where logdate > TO_TIMESTAMP ('2020-01-15 03:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and logdate < TO_TIMESTAMP ('2020-01-15 06:01:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and hostname like 'mo-8445329fd.lab-rot.saas.sap.corp' ) as newResult
left outer join
( select testscriptname,teststep, testreqname , avg(rqt) as avgrqt, (avg(rqt) * (1+ 0.3)) as avgrqtUp30Percent,  min(testiter) as maxtestiter, max(testiter) as maxtestiter, count(teststep) as teststepcount from 
                 (select * from SFQA05_TESTH_E.perflogbase where logdate > TO_TIMESTAMP ('2020-01-15 03:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and logdate < TO_TIMESTAMP ('2020-01-15 06:01:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and hostname like 'mo-8445329fd.lab-rot.saas.sap.corp' )  group by testscriptname, teststep, testreqname order by testscriptname, teststep, testreqname ) as summarizdAvgRQTTable
on newResult.testscriptname = summarizdAvgRQTTable.testscriptname and newResult.teststep = summarizdAvgRQTTable.teststep and newResult.testreqname = summarizdAvgRQTTable.testreqname 
where summarizdAvgRQTTable.avgrqtUp30Percent < newResult.rqt and newResult.rqt > 500 and not (newResult.gid = 'null')  order by (newResult.rqt - summarizdAvgRQTTable.avgrqt) desc 
                                            
 
     
/*Comparison Summary from request level*/                                       
select testscriptnamebase, teststepbase,testreqnamebase, avgrqtbase, avgrqt, diffRQT from 
 (select resultbase.testscriptnamebase, resultbase.teststepbase, resultbase.testreqnamebase, resultbase.avgrqtbase, resultnew.avgrqt, round((resultnew.avgrqt-resultbase.avgrqtbase)/(resultbase.avgrqtbase+1),2) as diffRQT
 from (select testscriptname as testscriptnamebase,teststep as teststepbase, testreqname as testreqnamebase,  avg(rqt) as avgrqtbase, min(testiter) as maxtestiterbase, max(testiter) as maxtestiterbase, count(teststep) as teststepcountbase from 
                 (select * from SFQA05_TESTH_E.perflogbase where logdate > TO_TIMESTAMP ('2020-01-15 03:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and logdate < TO_TIMESTAMP ('2020-01-15 04:30:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and hostname like 'mo-8445329fd.lab-rot.saas.sap.corp' )  group by testscriptname, teststep, testreqname order by testscriptname, teststep, testreqname) as resultbase
 left outer join                          
 (select testscriptname,teststep, testreqname , avg(rqt) as avgrqt, min(testiter) as maxtestiter, max(testiter) as maxtestiter, count(teststep) as teststepcount from 
                 (select * from SFQA05_TESTH_E.perflogbase where logdate > TO_TIMESTAMP ('2020-01-15 04:31:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and logdate < TO_TIMESTAMP ('2020-01-15 06:01:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and hostname like 'mo-8445329fd.lab-rot.saas.sap.corp' )  group by testscriptname, teststep, testreqname order by testscriptname, teststep, testreqname) as resultnew 
 on resultbase.testscriptnamebase = resultnew.testscriptname and resultbase.teststepbase = resultnew.teststep and resultbase.testreqnamebase = resultnew.testreqname order by resultbase.testscriptnamebase, resultbase.teststepbase, resultbase.testreqnamebase ) as summary where summary.diffRQT > 0.3 


/*All Selected gids/eids needs care*/
select analyzedTableEids.logdate, analyzedTableEids.gid, analyzedTableEids.eid, analyzedTableEids.url, analyzedTableEids.testscriptnamelast, 
       analyzedTableEids.teststeplast, analyzedTableEids.testreqnamelast, analyzedTableEids.avgrqtbase,  analyzedTableEids.avgrqt, analyzedTableEids.rqt, 
       analyzedTableEids.diffRQT, (analyzedTableEids.rqt - analyzedTableEids.avgrqt) as diffWithAvg
 from 
( select toCheckRequests.logdate, toCheckRequests.gid, toCheckRequests.eid, toCheckRequests.rqt, toCheckRequests.url, toCheckRequests.testscriptname as testscriptnamelast, toCheckRequests.teststep as teststeplast, 
        toCheckRequests.testreqname as testreqnamelast, diffselectedreqtable.avgrqtbase,  diffselectedreqtable.avgrqt, diffselectedreqtable.diffRQT from 
 ( select baseLast.logdate, baseLast.gid, baseLast.eid, baseLast.rqt, baseLast.url, baseLast.testscriptname as testscriptname, baseLast.teststep as teststep, 
        baseLast.testreqname as testreqname
from SFQA05_TESTH_E.perflogbase as baseLast  where baseLast.logdate > TO_TIMESTAMP ('2020-01-15 04:31:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and baseLast.logdate < TO_TIMESTAMP ('2020-01-15 06:01:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and baseLast.hostname like 'mo-8445329fd.lab-rot.saas.sap.corp' ) as toCheckRequests 
left outer join (
 select testscriptnamebase, teststepbase,testreqnamebase, avgrqtbase, avgrqt, diffRQT from 
 (select resultbase.testscriptnamebase, resultbase.teststepbase, resultbase.testreqnamebase, resultbase.avgrqtbase, resultnew.avgrqt, round((resultnew.avgrqt-resultbase.avgrqtbase)/(resultbase.avgrqtbase+1),2) as diffRQT
 from (select testscriptname as testscriptnamebase,teststep as teststepbase, testreqname as testreqnamebase,  avg(rqt) as avgrqtbase, min(testiter) as maxtestiterbase, max(testiter) as maxtestiterbase, count(teststep) as teststepcountbase from 
                 (select * from SFQA05_TESTH_E.perflogbase where logdate > TO_TIMESTAMP ('2020-01-15 03:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and logdate < TO_TIMESTAMP ('2020-01-15 04:30:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and hostname like 'mo-8445329fd.lab-rot.saas.sap.corp' )  group by testscriptname, teststep, testreqname order by testscriptname, teststep, testreqname) as resultbase
 left outer join                          
 (select testscriptname,teststep, testreqname , avg(rqt) as avgrqt, min(testiter) as maxtestiter, max(testiter) as maxtestiter, count(teststep) as teststepcount from 
                 (select * from SFQA05_TESTH_E.perflogbase where logdate > TO_TIMESTAMP ('2020-01-15 04:31:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and logdate < TO_TIMESTAMP ('2020-01-15 06:01:00', 'YYYY-MM-DD HH24:MI:SS') 
                                            and hostname like 'mo-8445329fd.lab-rot.saas.sap.corp' )  group by testscriptname, teststep, testreqname order by testscriptname, teststep, testreqname) as resultnew 
 on resultbase.testscriptnamebase = resultnew.testscriptname and resultbase.teststepbase = resultnew.teststep and resultbase.testreqnamebase = resultnew.testreqname order by resultbase.testscriptnamebase, resultbase.teststepbase, resultbase.testreqnamebase ) as summary where summary.diffRQT > 0.3 )
 as diffselectedreqtable 
  on toCheckRequests.testscriptname = diffselectedreqtable.testscriptnamebase and toCheckRequests.teststep = diffselectedreqtable.teststepbase and toCheckRequests.testreqname = diffselectedreqtable.testreqnamebase ) 
  as analyzedTableEids where analyzedTableEids.diffRQT > 0 and (analyzedTableEids.rqt - analyzedTableEids.avgrqt) > 0 and  not (analyzedTableEids.gid = 'null') order by (analyzedTableEids.rqt - analyzedTableEids.avgrqt) desc
