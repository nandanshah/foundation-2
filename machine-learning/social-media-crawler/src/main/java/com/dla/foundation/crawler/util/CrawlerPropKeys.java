package com.dla.foundation.crawler.util;

/**
 * Keys for properties file
 * 
 */
public enum CrawlerPropKeys {

	cassandraIPList("cassandraIPList"), cassandraPort("cassandraPort"), keySpace(
			"keySpace"), profilColumnFamily("profilColumnFamily"), socialProfileColumnFamily(
			"socialProfileColumnFamily"), friendsColumnFamily(
			"friendsColumnFamily"), inputPartitioner("inputPartitioner"), outputPartitioner(
			"outputPartitioner"), inputCQLPageRowSize("inputCQLPageRowSize"), sparkMaster(
			"sparkMaster"), sparkApName("sparkApName"), gigyaApiKey(
			"gigyaApiKey"), gigyaSecretKey("gigyaSecretKey"), gigyaApiScheme(
			"gigyaApiScheme"), gigyaApiDomain("gigyaApiDomain"), gigyaTimeoutMillis(
			"gigyaTimeoutMillis"), profileIdKey(
			"profileIdKey"), socialIdKey(
			"socialIdKey"), lastModifiedKey("lastModifiedKey"), frdsIdKeySep ("frdsIdKeySep");

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
