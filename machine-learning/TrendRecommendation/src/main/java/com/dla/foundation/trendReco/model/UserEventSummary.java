package com.dla.foundation.trendReco.model;

public enum UserEventSummary {

	TENANT("tenantid"), REGION("regionid"), ITEM("movieid"), EVENT_TYPE(
			"eventtype"), TIMESTAMP("timestamp"), AVP("avp"), USER("userid"), FLAG(
			"flag"), DATE("date");

	private String column;

	private UserEventSummary(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}
}
