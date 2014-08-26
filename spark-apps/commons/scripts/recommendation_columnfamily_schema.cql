create keyspace if not exists fis with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

use fis;

// Schema for trend

create table if not exists common_daily_eventsummary_per_useritem(periodid timeuuid ,tenantid uuid,regionid uuid,itemid uuid,profileid uuid,eventtypeaggregate  map<text,double>,dayscore double,date timestamp,eventrequired int,primary key (periodid,tenantid,regionid,itemid,profileid));

create index if not exists index_common_daily_eventsummary_per_useritem_event_required on common_daily_eventsummary_per_useritem (eventrequired);
create index if not exists index_common_daily_eventsummary_per_useritem_date on common_daily_eventsummary_per_useritem (date);

create table if not exists trend_daily_eventsummary(periodid timeuuid ,tenantid uuid,regionid uuid,itemid uuid,eventtypeaggregate  map<text,double>,dayscore double,date timestamp,eventrequired int,primary key (periodid,tenantid,regionid,itemid));

create index if not exists index_trend_daily_eventsummary_event_required on trend_daily_eventsummary (eventrequired);
create index if not exists index_trend_daily_eventsummary_date on trend_daily_eventsummary (date);

create table if not exists trend_reco(periodid timeuuid,tenantid uuid,regionid uuid,itemid uuid,trendscore double,normalizedtrendscore double,trendscorereason text,eventrequired int,date timestamp, primary key (periodid,tenantid,regionid,itemid));

create index if not exists index_trend_reco_event_required on trend_reco (eventrequired);
create index if not exists index_trend_reco_date on trend_reco (date);

// Schema for f&p

create table if not exists fnp_reco(periodid timeuuid,tenantid uuid,regionid uuid,itemid uuid,fnpscore double,normalizedfnpscore double,fnpscorereason text,eventrequired int,date timestamp, primary key (periodid,tenantid,regionid,itemid));

create index if not exists index_f&p_reco_event_required on fnp_reco (eventrequired);
create index if not exists index_f&p_reco_date on fnp_reco (date);



// Schema for new release

create table if not exists new_release_reco(periodid timeuuid,tenantid uuid,regionid uuid,itemid uuid,newreleasescore double,normalizednewreleasescore double,newreleasescorereason text,eventrequired int,date timestamp, primary key (periodid,tenantid,regionid,itemid));

create index if not exists index_new_release_reco_event_required on new_release_reco (eventrequired);
create index if not exists index_new_release_reco_date on new_release_reco (date);



// Schema for popularity

create table if not exists popularity_reco(periodid timeuuid,tenantid uuid,regionid uuid,itemid uuid,popularityscore double,normalizedpopularityscore double,popularityscorereason text,eventrequired int,date timestamp,primary key (periodid,tenantid,regionid,itemid));

create index if not exists popularity_reco_event_required on popularity_reco (eventrequired);
create index if not exists popularity_reco_date on popularity_reco (date);

// Schema for user item reco column family 

create table if not exists user_item_score_summary (periodid timeuuid,tenantid uuid,regionid uuid,itemid uuid,trendscore double,trendscorereason text,popularityscore double,popularityscorereason text,fnpscore double,fnpscorereason text,newreleasescore double,newreleasescorereason text,eventrequired int,date timestamp,primary key (periodid,tenantid, regionid,itemid));

create index if not exists user_item_score_summary_event_required on user_item_score_summary (eventrequired);
create index if not exists user_item_score_summary_date on user_item_score_summary (date);

create table if not exists user_item_reco(periodid timeuuid,tenantid uuid,regionid uuid,profileid uuid,itemid uuid,trendscore double,trendscorereason text,popularityscore double,popularityscorereason text,fnpscore double,fnpscorereason text,newreleasescore double,newreleasescorereason text,socialscore double,socialscorereason text,recobyfoundationscore double,recobyfoundationreason text,eventrequired int,date timestamp,primary key (periodid,tenantid, regionid,profileid,itemid));
			  
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
              recobyfoundationreason text, eventrequired int,primary key(profileid,itemid));

create index if not exists pio_user_reco_event_required on pio_user_reco (eventrequired);
create index if not exists pio_user_reco_date on pio_user_reco (date);


// Schema for EO 
create table if not exists eo_spark_app_prop(sparkappname text, properties map<text,text>, primary key (sparkappname));
create table if not exists eo_event_metadata(id uuid,eventtype  text,required  int, primary key (id));
create index if not exists eo_event_metadata_event_type on eo_event_metadata (eventtype);

insert into eo_event_metadata (id, eventtype , required ) VALUES ( bd978e9f-4169-461b-883c-e60440c4f5b3, 'ProfileAdded', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 98352967-a614-4823-a400-a527e8ba630a, 'ProfileDeleted', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 2e8c17c8-1d0f-40ef-bac4-ea3aa02ad98f, 'ProfileUpdatePreferredRegion', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( da667be8-6c8b-4aa4-90ac-a1cb128e8818, 'ProfileUpdatePreferredLocale', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( fcc87a24-dc33-4016-98e1-e502c20886f5, 'ProfileUpdateNewSocialMediaAccountLinkage', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 909f2cf6-d6db-4c6e-829e-8c608a6233ee, 'ProfileUpdateSocialMediaAccountDelete', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 88f04a3a-81ff-48fe-ae4f-8d01f159803b, 'AccountAdd', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 7100b484-9774-405a-94cb-8a3f6273f8d7, 'AccountDelete', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 84edf7c0-28c9-442a-bb92-a1635cda1d33, 'AccountInfoUpdate', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 7883e709-bc77-44e0-aa9e-31ea62e31601, 'UserLogin', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 4c92b814-a344-4d21-abfe-a68225d2e1db, 'UserLogout', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 82ac8967-c5df-4d37-8a21-f6640cb8dd5a, 'AddSession', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( beb3448d-1981-4245-b3cd-7b1c47f2b2a1, 'UserSearch', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 811daa04-b711-4148-a418-e10f2983ed34, 'UserSearchResultClick', 1) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 8ed22772-4820-4b53-9de0-8c494333e219, 'UserItemPreview', 1) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( eba3b522-fbd1-4b55-bb5e-52e9b83bf4bc, 'UserItemMoreInfo', 1) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( d41b5e62-6f1e-4458-a58f-1bbd9f2435fd, 'UserItemShare', 1) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 05880361-3345-406c-832b-bfc28e7710ff, 'UserItemRate', 1) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 8e37d4ce-4630-44c2-b6db-3021a3c41c72, 'UserItemAddToWatchList', 1) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( a34fa67b-fc09-474b-9d87-b825df1e4da2, 'UserItemDeleteFromWatchlist', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 8be49b92-80ff-4a2c-9d17-c02bbc4290df, 'UserItemPlayStart', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( d8e73f5c-0726-499e-94d0-e27b75e23eae, 'UserItemPlayPercentage', 1) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( b4383385-10a6-4c8f-a3cf-d37f443e310f, 'UserItemPlayStop', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( e5f4326d-71d8-43c0-9da8-cf8e3c662b26, 'UserItemPlayPause', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( ea06c052-15c5-4439-930a-84f5ae4cf565, 'UserItemPlayResume', 0) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 5a1fba63-abc1-41f0-af0c-31a6a6a49581, 'UserItemRent', 1) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( e1df2f93-e422-4477-8501-a19f418140a5, 'UserItemPurchase', 1) ;
insert into eo_event_metadata (id, eventtype , required ) VALUES ( 5c0cf1b4-5571-4f6c-81f5-167ce7032a8f, 'UserItemImpression', 1) ;

