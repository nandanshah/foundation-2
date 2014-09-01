package com.dla.foundation.connector.util;

public enum UserRecoSummary {

	TENANT("tenantid"), REGION("regionid"), ITEM("itemid"), TRENDSCORE(
			"trendscore"), SOCIALSCORE("socialscore"), POPULARITYSCORE("popularityscore"), FPSCORE("fnpscore"), NEWSCORE(
			"newreleasescore"), RECOBYFOUNDATIONSCORE("recobyfoundationscore"),DATE("date"), ID("periodid"), USER("profileid"), TRENDREASON("trendscorereason"), POULARITYREASON("popularityscorereason"), 
			SOCIALREASON("socialscorereason"), FPREASON("fnpreason"), NEWREASON("newreleasescorereason"), RECOBYFOUNDATIONREASON("recobyfoundationscorereason");

	private String column;

	private UserRecoSummary(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}
}
