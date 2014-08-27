package com.dla.foundation.trendReco.model;

public enum UserEventSummary {

	TENANT("tenantid"), REGION("regionid"), ITEM("itemid"), EVENT_TYPE(
			"eventtype"), TIMESTAMP("timestamp"), PLAY_PERCENTAGE("playpercentage"), RATE_SCORE("ratescore"), PROFILE("profileid"), EVENTREQUIRED(
			"eventrequired"), DATE("date");

	private String column;

	private UserEventSummary(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}
}
