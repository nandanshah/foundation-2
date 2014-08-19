package com.dla.foundation.crawler.util;

/**
 * Keys for properties file
 * 
 */
public enum CrawlerPropKeys {

	cassandraIPList("cs_hostList"), cassandraPort("cs_rpcPort"), fisKeyspace(
			"cs_fisKeyspace"),analyticsKeyspace("cs_analyticsKeyspace") , profilColumnFamily("profilColumnFamily"), socialProfileColumnFamily(
			"socialProfileColumnFamily"), friendsColumnFamily(
			"friendsColumnFamily"), inputPartitioner("inputPartitioner"), outputPartitioner(
			"outputPartitioner"), inputCQLPageRowSize("cs_pageRowSize"), sparkMaster(
			"spark_host"), gigyaApiKey(
			"gigyaApiKey"), gigyaSecretKey("gigyaSecretKey"), gigyaApiScheme(
			"gigyaApiScheme"), gigyaApiDomain("gigyaApiDomain"), gigyaTimeoutMillis(
			"gigyaTimeoutMillis"), profileIdKey(
			"profileIdKey"), socialIdKey(
			"socialIdKey"), lastCrawlerRunTimeKey("lastCrawlerRunTimeKey");

	private String value;

	CrawlerPropKeys(String value) {
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
