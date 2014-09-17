package com.dla.foundation.crawler.util;

/**
 * Keys for properties file
 * 
 */
public enum CrawlerPropKeys {

	OUT_DATED_THRESHOLD_TIME("lastcrawlerruntime");

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
