Trend Recommendation Project:
This project provides the functionality of calculating trend score for the items.In the process of calculating trend score
it computes user summary and daily event summary.

This project contains 3 modules which are:
  1. Calculating user summary (Provides incremental & Recalculation approach independently).	              															
  2. Calculating daily event summary (Provides incremental & Recalculation approach independently).
  3. Calculating Trend score.(Provides incremental & Recalculation approach independently).

Cassandra Schema used:

1. CREATE KEYSPACE trendreco WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
2. use trendreco;
3. create table usereventsummary (id uuid, tenantid int, regionid int , userid int, movieid int, eventtype int,  sessionid int,  timestamp bigint, avp map<text,varchar>, flag int,date timestamp,primary key (id,tenantid, regionid, userid, movieid));
4. CREATE INDEX trendrecousereventsummaryflag ON usereventsummarytable (flag);
5. CREATE INDEX trendrecousereventsummarydate ON usereventsummarytable (date);
6. create table dailyeventsummaryperuseritem(periodid timeuuid ,tenantid int,regionid int,itemid int,userid int,eventtypeaggregate  map<int,int>,dayscore double,date timestamp,flag int,primary key (periodid,tenantid,regionid,itemid,userid));
7. CREATE INDEX trendrecodailyeventsummaryperuseritemflag ON dailyeventsummaryperuseritem (flag);
8. CREATE INDEX trendrecodailyeventsummaryperuseritemdate ON dailyeventsummaryperuseritem (date);
9. create table dailyeventsummary(periodid timeuuid ,tenantid int,regionid int,itemid int,eventtypeaggregate  map<int,int>,dayscore double,date timestamp,flag int,primary key (periodid,tenantid,regionid,itemid));
10. CREATE INDEX trendrecodailyeventsummaryflag ON dailyeventsummary (flag);
11. CREATE INDEX trendrecodailyeventsummarydate ON dailyeventsummary (date);
12. create table trend(id timeuuid,tenantid int,regionid int,itemid int,trendscore double,normalizedscore double,primary key (id,tenantid,regionid,itemid));

Sample property files are provided to understand how property should be provided.
JUNIT's are provided for all the 3 modules.
In order to run JUNIT,you need to have cassandra set up with localhost as hostname or if there is any ip for cassandra, then you need to replace localhost 
with ip in appPropTest file present in src/main/resources and in all the junits present in src/main/tests.
