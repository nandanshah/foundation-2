package com.dla.foundation.model;

public enum Profile {

	id("id"), socialauthtoken("socialauthtoken"), lastcrawlerruntime("lastcrawlerruntime"),
	accountid("accountid"), isSocialCrawlRequired("isSocialCrawlRequired");

	private String value;

	Profile(String value) {
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
