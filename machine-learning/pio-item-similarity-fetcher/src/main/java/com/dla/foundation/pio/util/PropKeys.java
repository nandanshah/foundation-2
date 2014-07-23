package com.dla.foundation.pio.util;

public enum PropKeys {

	HOSTNAME("hostname"),  //PIO Hostname
	APPKEY("appkey"), 	   //PIO App key 
	ENGINE("engine"),	   //PIO Engine Name
	PIOPort("PIOPort"),	   //PIO API Port
	NUM_REC_PER_ITEM("numRecsPerUser"), //Number of similar items to be fetched for each item.
	SPARK_MASTER("sparkMaster"),  //Spark Master Mode
	SPARK_APPNAME("sparkAppName"),// Spark App Name
	CASSANDRA_PORT("cassendraPort"),// Cassandra Port.
	KEYSPACE("keySpace"),  // Cassndra keyspace in use. 
	DESTINATION_CF("destinationCF"), // Target Similarity Table
	INPUT_PARTITIONER("inputPartitioner"), // Cassandra Input Partitioner
	OUTPUT_PARTITIONER("outputPartitioner"),// Cassandra Output Partitioner
	DESTINATION_PK("destinationPK"),  //Target Similarity Table's column name that will hold itemids 
	DESTINATION_SIM_COL("destinationSimilarityCol"), //Target Similarities Table's column name that will hold list of similarities.
	DESTINATION_TIMESTAMP_COL("destinationTimeStampCol"),//Target Similarities Table's column name that will hold Timestamp, when last time similarities were fetched.
	CASSANDRA_IP("cassandraIP"),// Cassandra IPs 
	SOURCE_KEY("sourceKey"),// Source Cassandra Table's column name which holds itemids . 
	SOURCE_CF("sourceCF"),// Source Cassandra Table.
	CASSANDRA_DBHOST("cassandraDBHost"), // Cassandra Database host.
	PAGE_ROW_SIZE("pageRowSize"); // Page Row Size.

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
