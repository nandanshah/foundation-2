package com.dla.foundation.connector.util;

public enum UserRecoSummary {

	TENANT("tenantid"), REGION("regionid"), ITEM("itemid"), TRENDSCORE(
			"trendscore"), SOCIALSCORE("socialscore"), POPULARITYSCORE("popularityscore"), FPSCORE("fpscore"), NEWSCORE(
			"newscore"), DATE("date"), ID("id"), USER("userid"), TRENDREASON("trendreason"), POULARITYREASON("popularityreason"), 
			SOCIALREASON("socialreason"), FPREASON("fpreason"), NEWREASON("newreason"), RECOBYFOUNDATIONREASON("recoByfoundationreason");

	private String column;

	private UserRecoSummary(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}
}
