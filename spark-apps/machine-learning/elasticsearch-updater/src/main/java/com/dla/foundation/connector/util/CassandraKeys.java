package com.dla.foundation.connector.util;

public enum CassandraKeys {

	MODE_PROPERTY("spark_mode"), APP_NAME("app_name"), INPUT_KEYSPACE(
			"input_keyspace"), INPUT_COLUMNFAMILY("input_columnfamily"), INPUT_PARTITIONER(
			"input_partitioner"), INPUT_RPC_PORT("rpc_port"), OUTPUT_KEYSPACE(
			"output_keyspace"), OUTPUT_COLUMNFAMILY("output_columnfamily"), OUTPUT_PARTITIONER(
			"output_partitioner"), PAGE_ROW_SIZE("page_row_size"), EVENT_REQUIRED(
			"event_required"), INPUT_DATE("input_date"), INPUT_HOST_LIST(
			"read_host_list"), OUTPUT_HOST_LIST("write_host_list");

	private String value;

	CassandraKeys(String value) {
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
