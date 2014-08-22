package com.dla.foundation.trendReco.model;

public enum Trend {
	ID("id"), TENANT("tenantid"), REGION("regionid"), ITEM("itemid"), TREND_SCORE(
			"trendscore"), NORMALIZED_SCORE("normalizedtrendscore"), TREND_SCORE_REASON(
			"trendscorereason"),EVENTREQUIRED("eventrequired"), DATE("date");		

	private String column;

	private Trend(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}

}
