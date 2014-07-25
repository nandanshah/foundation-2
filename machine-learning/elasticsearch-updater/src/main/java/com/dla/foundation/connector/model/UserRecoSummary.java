package com.dla.foundation.connector.model;

public enum UserRecoSummary {

	TENANT("tenantid"), REGION("regionid"), ITEM("itemid"), TRENDSCORE(
			"trendscore"), SOCIALSCORE("socialscore"), POPULARITYSCORE("popularityscore"), FPSCORE("fpscore"), NEWSCORE(
			"newscore"), DATE("date"), ID("id"), USER("userid");

	private String column;

	private UserRecoSummary(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}
}
