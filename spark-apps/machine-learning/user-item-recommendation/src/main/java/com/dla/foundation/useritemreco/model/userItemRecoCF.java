package com.dla.foundation.useritemreco.model;

public enum userItemRecoCF {
	ID("id"), PROFILE("profileid"), TENANT("tenantid"), REGION("regionid"), ITEM(
			"itemid"), NORMALIZED_TREND_SCORE("normalizedtrendscore"), TREND_SCORE(
					"trendscore"), TREND_SCORE_REASON("trendscorereason"), NORMALIZED_POPULARITY_SCORE(
							"normalizedpopularityscore"), POPULARITY_SCORE("popularityscore"), POPULARITY_SCORE_REASON(
									"popularityscorereason"),NORMALIZED_FP_SCORE("normalizedfpscore"),FP_SCORE(
											"fpscore"), FP_SCORE_REASON("fpscorereason"),NORMALIZED_NEW_SCORE("normalizednewscore"),
											NEW_RELEASE_SCORE("newscore"), NEW_RELEASE_SCORE_REASON("newscorereason"),
											SOCIAL_SCORE("socialscore"),SOCIAL_SCORE_REASON("socialscorereason"),
											PIO_SCORE("recoByfoundation"),PIO_SCORE_REASON("recoByfoundationscorereason"),
											DATE("date"), EVENT_REQUIRED("eventrequired"), PREFERRED_REGION("preferredregion"), ACCOUNT("accountid")
											,PIO_DATE("lastrecofetched");
	private String column;

	private userItemRecoCF(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}
}
