package com.dla.foundation.analytics.utils;

/**
 * Enum to handle common properties across all apps
 * 
 */

public enum CommonPropKeys {

	// cassandra common properties
	cs_hostList("cs_hostList"), cs_rpcPort("cs_rpcPort"), cs_pageRowSize(
			"cs_pageRowSize"), cs_fisKeyspace("cs_fisKeyspace"), cs_platformKeyspace(
			"cs_platformKeyspace"), cs_platformEntityPackagePrefix(
			"cs_platformEntityPackagePrefix"), cs_fisEntityPackagePrefix(
					"cs_fisEntityPackagePrefix"), cs_sparkAppPropCF("cs_sparkAppPropCF"), cs_sparkAppPropCol(
			"cs_sparkAppPropCol"),cs_profileCF("cs_profileCF"), cs_accountCF(
					"cs_accountCF"), cs_itemCF("cs_itemCF"),

	// spark common properties
	spark_host("spark_host"), spark_port("spark_port"),

	// PIO common properties
	pio_host("pio_host"), pio_port("pio_port"),pio_appkey("pio_appkey"),pio_engine("pio_engine"),

	// Elastic search common properties
	es_host("es_host"), es_httpPort("es_httpPort"), es_transportPort(
			"es_transportPort"), es_clusterName("es_clusterName"), es_destTimeStampCol(
			"es_destTimeStampCol"), es_index_name("es_index_name"), es_movie_index_type("es_movie_index_type"),
			es_userreco_index_type("es_userreco_index_type"),

	// gigya common properties;
	gigya_ApiKey("gigya_ApiKey"), gigya_SecretKey("gigya_SecretKey"), gigya_ApiScheme(
			"gigya_ApiScheme"), gigya_ApiDomain("gigya_ApiDomain"), gigya_TimeoutMillis(
			"gigya_TimeoutMillis");

	private String value;

	private CommonPropKeys(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
