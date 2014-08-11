package com.dla.foundation.analytics.utils;

/**
 * Enum to handle common properties across all apps
 * 
 */
public enum CommonPropKeys {

	// cassandra common properties
	cs_hostList("cs_hostList"), cs_rpcPort("cs_rpcPort"), cs_pageRowSize(
			"cs_pageRowSize"), cs_fisKeyspace("cs_fisKeyspace"), cs_analyticsKeyspace(
			"cs_analyticsKeyspace"), cs_entityPackagePrefix(
			"cs_entityPackagePrefix"),

	// spark common properties
	spark_host("spark_host"), spark_port("spark_port"),

	// PIO common properties
	pio_host("pio_host"), pio_port("pio_port"), pio_appkey("pio_appkey"), pio_engine(
			"pio_engine"),

	// Elastic search common properties
	es_host("es_host"), es_httpPort("es_httpPort"), es_transportPort(
			"es_transportPort"), es_clusterName("es_clusterName"), es_destTimeStampCol(
			"es_destTimeStampCol");

	private String key;

	private CommonPropKeys(String key) {
		this.key = key;
	}

	public String getValue() {
		return key;
	}
}