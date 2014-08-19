package com.dla.foundation.pio.util;

public enum PropKeys {

	HOSTNAME("hostname"),  //PIO Hostname
	APPKEY("appkey"), 	   //PIO App key 
	ENGINE("engine"),	   //PIO Engine Name
	PIOPort("PIOPort"),	   //PIO API Port
	NUM_REC_PER_USER("numRecsPerUser"), //Number of recommendations to be fetched for each user.
	SPARK_MASTER("sparkMaster"),  //Spark Master Mode
	SPARK_APPNAME("sparkAppName"),// Spark App Name
	CASSANDRA_PORT("cassendraPort"),// Cassandra Port.
	KEYSPACE("keySpace"),  // Cassndra keyspace in use. 
	RECOMMEND_CF("recommendCF"), // Target Recommendation Table
	INPUT_PARTITIONER("inputPartitioner"), // Cassandra Input Partitioner
	OUTPUT_PARTITIONER("outputPartitioner"),// Cassandra Output Partitioner
	RECOMMEND_USERIDKEY("recommendKey"),  //Target Recommendation Table's column name that will hold Userids 
	RECOMMEND_REC_COL("recommendRecCol"), //Target Recommendation Table's column name that will hold Recommendations.
	RECOMMEND_TIMESTAMP_COL("recommendTimeStampCol"),//Target Recommendation Table's column name that will hold Timestamp, when last time recommendations were fetched.
	CASSANDRA_IP("cassandraIP"),// Cassandra IPs 
	PROFILE_USERIDKEY("profileUserKey"),// Source Cassandra profile Table's column name which holds userid. 
	PROFILE_CF("profileCF"),// Source Cassandra profile Table.
	CASSANDRA_DBHOST("cassandraDBHost"), // Cassandra Database host.
	PAGE_ROW_SIZE("pageRowSize"), // Page Row Size.
	ACCOUNT_CF("accountCF"),
	ACCOUNT_KEY("accountKey");

	private String value;

	PropKeys(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return value;
	}

}
