package com.dla.foundation.trendReco.model;

public enum DailyEventSummaryPerItem {
	PERIOD("periodid"), TENANT("tenantid"), REGION("regionid"), ITEM("itemid"), EVENT_TYPE(
			"eventtype"), DAY_SCORE("dayscore"), EVENT_AGGREGATE(
			"eventtypeaggregate"), EVENTREQUIERD("eventrequired"), DATE("date");

	private String column;

	private DailyEventSummaryPerItem(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}
}
