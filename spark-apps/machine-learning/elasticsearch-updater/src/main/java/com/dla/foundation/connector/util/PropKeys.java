package com.dla.foundation.connector.util;

public enum PropKeys {

	MODE_PROPERTY("spark_host"), APP_NAME("app_name"), INPUT_KEYSPACE(
			"cs_fisKeyspace"), INPUT_COLUMNFAMILY("input_columnfamily"), INPUT_PARTITIONER(
			"input_partitioner"), INPUT_RPC_PORT("cs_rpcPort"), OUTPUT_KEYSPACE(
			"output_keyspace"), OUTPUT_COLUMNFAMILY("output_columnfamily"), OUTPUT_PARTITIONER(
			"output_partitioner"), PAGE_ROW_SIZE("cs_pageRowSize"), EVENT_REQUIRED(
			"event_required"), INPUT_DATE("input_date"), INPUT_HOST_LIST(
			"cs_hostList"), OUTPUT_HOST_LIST("cs_hostList"), RECAL_START_DATE(
			"recalculation_start_date"), RECAL_END_DATE(
			"recalculation_end_date"), RECAL_PERIOD("recalculation_period"), INCREMENTAL_FLAG(
			"incremental_flag"), ZSCORE_PERIOD("zscore_period"), CURRENT_TREND_DATE(
			"current_trend_date"), FLAG("flag"), DATE("date"), FULL_QUALIFIED_CLASSNAME(
			"fully_qualified_classname"),ES_HOST("es_host"),ES_PORT("es_httpPort");

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