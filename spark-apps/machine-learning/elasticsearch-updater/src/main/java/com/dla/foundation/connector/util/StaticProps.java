package com.dla.foundation.connector.util;

public enum StaticProps {

	APP_NAME("CassandraESConnector"),INPUT_PARTITIONER("Murmur3Partitioner"),OUTPUT_PARTITIONER("Murmur3Partitioner"),
	INPUT_COLUMNFAMILY("user_item_reco"),
	CREATE_INDEX("true"),COUNT_THRESHHOLD("9"),SCHEMA_PATH1("userRecoSchema_1.json"),SCHEMA_PATH2("userrecoSchema_2.json"),SCHEMA_PATH3("Reco_type.json"),INPUT_DATE("input_date");//URL_HOST("http://localhost:9200/"),SPARK_MODE("local")
	
	
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