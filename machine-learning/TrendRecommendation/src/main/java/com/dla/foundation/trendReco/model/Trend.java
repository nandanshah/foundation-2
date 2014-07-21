package com.dla.foundation.trendReco.model;

public enum Trend {
	ID("id"), TENANT("tenantid"), REGION("regionid"), ITEM("itemid"), TREND_SCORE(
			"trendscore"), NORMALIZED_SCORE("normalizedscore");

	private String column;

	private Trend(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}

}
