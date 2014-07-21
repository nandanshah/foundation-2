package com.dla.foundation.trendReco.model;

public enum DailyEventSummaryPerUserItem {

	PERIOD("periodid"), TENANT("tenantid"), REGION("regionid"), ITEM("itemid"), EVENT_AGGREGATE(
			"eventtypeaggregate"), FLAG("flag"), DATE("date"), DAY_SCORE(
			"dayscore"), USER("userid");

	private String column;

	private DailyEventSummaryPerUserItem(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}
}
