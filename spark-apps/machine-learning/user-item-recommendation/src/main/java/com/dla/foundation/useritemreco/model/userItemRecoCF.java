package com.dla.foundation.useritemreco.model;

public enum userItemRecoCF {
	ID("id"), USER_ID("userid"), TENANT("tenantid"), REGION("regionid"), ITEM(
			"itemid"), NORMALIZED_TREND_SCORE("normalizedtrendscore"), TREND_SCORE(
			"trendscore"), TREND_SCORE_REASON("trendscorereason"), NORMALIZED_POPULARITY_SCORE(
			"normalizedpopularityscore"), POPULARITY_SCORE("popularityscore"), POPULARITY_SCORE_REASON(
			"popularityscorereason"), DATE("date"), FLAG("flag"), PREFERRED_REGION(
			"preferredregion"), ACCOUNT("accountid");
	private String column;

	private userItemRecoCF(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}
}
