create keyspace if not exists fis with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

use fis;

// Schema for trend

create table if not exists common_daily_eventsummary_per_useritem(periodid timeuuid ,tenantid uuid,regionid uuid,itemid uuid,profileid uuid,eventtypeaggregate  map<text,int>,dayscore double,date timestamp,eventrequired int,primary key (periodid,tenantid,regionid,itemid,profileid));

create index if not exists index_common_daily_eventsummary_per_useritem_event_required on common_daily_eventsummary_per_useritem (eventrequired);
create index if not exists index_common_daily_eventsummary_per_useritem_date on common_daily_eventsummary_per_useritem (date);

create table if not exists trend_daily_eventsummary(periodid timeuuid ,tenantid uuid,regionid uuid,itemid uuid,eventtypeaggregate  map<text,int>,dayscore double,date timestamp,eventrequired int,primary key (periodid,tenantid,regionid,itemid));

create index if not exists index_trend_daily_eventsummary_event_required on trend_daily_eventsummary (eventrequired);
create index if not exists index_trend_daily_eventsummary_date on trend_daily_eventsummary (date);

create table if not exists trend_reco(periodid timeuuid,tenantid uuid,regionid uuid,itemid uuid,trendscore double,normalizedtrendscore double,trendscorereason text,eventrequired int,date timestamp, primary key (periodid,tenantid,regionid,itemid));

create index if not exists index_trend_reco_event_required on trend_reco (eventrequired);
create index if not exists index_trend_reco_date on trend_reco (date);

// Schema for f&p

create table if not exists f&p_reco(periodid timeuuid,tenantid uuid,regionid uuid,itemid uuid,trendscore double,normalizedf&pscore double,f&pscorereason text,eventrequired int,date timestamp, primary key (periodid,tenantid,regionid,itemid));

create index if not exists index_f&p_reco_event_required on f&p_reco (eventrequired);
create index if not exists index_f&p_reco_date on f&p_reco (date);



// Schema for new release

create table if not exists new_release_reco(periodid timeuuid,tenantid uuid,regionid uuid,itemid uuid,newreleasescore double,normalizednewreleasescore double,newreleasescorereason text,eventrequired int,date timestamp, primary key (periodid,tenantid,regionid,itemid));

create index if not exists index_new_release_reco_event_required on new_release_reco (eventrequired);
create index if not exists index_new_release_reco_date on new_release_reco (date);



// Schema for popularity

create table if not exists popularity_reco(periodid timeuuid,tenantid uuid,regionid uuid,itemid uuid,popularityscore double,normalizedpopularityscore double,popularityscorereason text,eventrequired int,date timestamp,primary key (periodid,tenantid,regionid,itemid));

create index if not exists popularity_reco_event_required on popularity_reco (eventrequired);
create index if not exists popularity_reco_date on popularity_reco (date);

// Schema for user item reco column family 

create table if not exists user_item_score_summary (periodid timeuuid,tenantid uuid,regionid uuid,itemid uuid,trendscore double,trendscorereason text,popularityscore double,popularityscorereason text,eventrequired int,date timestamp,primary key (periodid,tenantid, regionid,itemid));

create index if not exists user_item_score_summary_event_required on user_item_score_summary (eventrequired);
create index if not exists user_item_score_summary_date on user_item_score_summary (date);

create table if not exists user_item_reco(periodid timeuuid,tenantid uuid,regionid uuid,profileid uuid,itemid uuid,trendscore double,trendscorereason text,popularityscore double,popularityscorereason text,socialscore double,socialscorereason text,recobyfoundationscore double,
              recobyfoundationreason text,eventrequired int,date timestamp,primary key (periodid,tenantid, regionid,profileid,itemid));
			  
create index if not exists user_item_reco_event_required on user_item_reco (eventrequired);
create index if not exists user_item_reco_date on user_item_reco (date);

// Schema for social

create table if not exists social_friends_info( profileid uuid,friendprofileid uuid , relation text, primary key(profileid,friendid));

create table if not exists social_profile(id uuid, name text, username text, location text, gender text, country text, timezone text, primary key(id));

create table if not exists social_reco(periodid timeuuid, tenantid uuid,regionid uuid,itemid uuid, profileid uuid,socialscore double,socialscorereason text, eventrequired int,date timestamp,primary key (periodid,tenantid,regionid,itemid,profileid));

create index if not exists social_reco_event_required on social_reco (eventrequired);
create index if not exists social_reco_date on social_reco (date);


// Schema for Pio

create table  pio_user_reco ( tenantid uuid, regionid uuid, profileid uuid, itemid uuid ,lastrecofetched timestamp, recobyfoundationscore double,
              recobyfoundationreason text, eventrequired int,date timestamp,primary key(profileid,itemid));

create index if not exists pio_user_reco_event_required on pio_user_reco (eventrequired);
create index if not exists pio_user_reco_date on pio_user_reco (date);


// Schema for EO 
create table if not exists eo_spark_app_prop(sparkappname text, properties map<text,text>, primary key (sparkappname));
create table if not exists eo_event_metadata(id uuid,eventtype  text,required  int, primary key (id));
create index if not exists eo_event_metadata_event_type on eo_event_metadata (eventtype);
