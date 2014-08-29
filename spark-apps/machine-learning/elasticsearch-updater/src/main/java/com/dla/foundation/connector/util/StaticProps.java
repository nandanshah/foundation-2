package com.dla.foundation.connector.util;

public enum StaticProps {

	APP_NAME("CassandraESConnector"),INPUT_PARTITIONER("Murmur3Partitioner"),OUTPUT_PARTITIONER("Murmur3Partitioner"),SPARK_MODE("local"),
	INPUT_COLUMNFAMILY("user_item_reco"),
	CREATE_INDEX("true"),COUNT_THRESHHOLD("10"),SCHEMA_PATH1("src/main/resources/userRecoSchema_1.json"),SCHEMA_PATH2("src/main/resources/userrecoSchema_2.json");//URL_HOST("http://localhost:9200/")
	
	
	String value;
	StaticProps(String value){
		this.value=value;
	}
	
	public String getValue() {
		return value;
	}
	
	@Override
	public String toString() {
		return value;
	}
}