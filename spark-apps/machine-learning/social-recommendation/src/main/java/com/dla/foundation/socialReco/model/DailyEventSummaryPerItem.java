package com.dla.foundation.socialReco.model;

public enum DailyEventSummaryPerItem {
	
	PERIOD("periodid"), TENANT("tenantid"), REGION("regionid"), ITEM("itemid"), USER("profileid"), 
		EVENT_TYPE("eventtype"), DAY_SCORE("dayscore"), EVENT_AGGREGATE(
			"eventtypeaggregate"), FLAG("eventrequired"), DATE("date");

	private String column;

	private DailyEventSummaryPerItem(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}

	@Override
	public String toString() {
		return column;
	}
}
