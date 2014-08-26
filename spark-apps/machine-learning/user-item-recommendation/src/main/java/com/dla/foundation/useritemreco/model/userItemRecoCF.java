package com.dla.foundation.useritemreco.model;

public enum userItemRecoCF {
	ID("id"), PROFILE("profileid"), TENANT("tenantid"), REGION("regionid"), ITEM(
			"itemid"), NORMALIZED_TREND_SCORE("normalizedtrendscore"), TREND_SCORE(
			"trendscore"), TREND_SCORE_REASON("trendscorereason"), NORMALIZED_POPULARITY_SCORE(
			"normalizedpopularityscore"), POPULARITY_SCORE("popularityscore"), POPULARITY_SCORE_REASON(
			"popularityscorereason"), NORMALIZED_FP_SCORE("normalizedfnpscore"), FP_SCORE(
			"fnpscore"), FP_SCORE_REASON("fnpscorereason"), NORMALIZED_NEW_SCORE(
			"normalizednewreleasescore"), NEW_RELEASE_SCORE("newreleasescore"), NEW_RELEASE_SCORE_REASON(
			"newreleasescorereason"), SOCIAL_SCORE("socialscore"), SOCIAL_SCORE_REASON(
			"socialscorereason"), PIO_SCORE("recobyfoundationscore"), PIO_SCORE_REASON(
			"recoByfoundationreason"), DATE("date"), EVENT_REQUIRED(
			"eventrequired"), PREFERRED_REGION("preferredregion"), ACCOUNT(
			"accountid"), PIO_DATE("lastrecofetched"), PERIOD_ID("periodid");
	private String column;

	private userItemRecoCF(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}
}
