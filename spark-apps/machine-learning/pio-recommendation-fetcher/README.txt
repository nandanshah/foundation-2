Requirement: Implementation of PIO-Recommendations fetcher for pre-fetched list of users and writing results to Cassandra table/Text file.

Target Cassandra Table Schema: 
	CREATE TABLE piorec ( profileid uuid,recommendations list<uuid>,lastupdate timestamp, PRIMARY KEY (profileid));

Source Cassndra Table Schema:
	CREATE TABLE profile (accountid uuid, id uuid, avatarimageurl text,birthdate timestamp,defaultcolor text,emailaddress text,emailaddress_lower text,firstname text,gender text,isadmin boolean,isrootprofile boolean,lastname text,notfiyfriendregistration boolean,notifyfriendlike boolean,notifynewrelease boolean,notifypromotional boolean,notifywatchlistexpiration boolean,passwordhash text,passwordsalt text,pinhash text,pinsalt text,profiletype text,socialauthtoken text,watchlist_id uuid,PRIMARY KEY (accountid, id));


Running JUNIT :

1. I have created a Sample1 app on Prediction IO host.
	These are the details : 
	appkey = pmLOX0oDU9aS7JmBhaUBDuwXSs7gBJVrNRLKoGfEENm3TeBj5IhiSTYarfXxK045
	engine = Sample
	hostname = 10.0.140.64
2. Engine is already trained. No need to train it again. Just execute JUnit. Training engine again may cause to change in results. 
3. JUnit will verify if code is fetching recommendations for some predetermined users. I have determined these users by running code several time. Found users for those PIO returning recommendations. Used same userids for assert test.