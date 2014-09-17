package com.dla.foundation.socialReco.model;

public enum Social {
	ID("periodid"), TENANT("tenantid"), REGION("regionid"), ITEM("itemid"), 
	USER("profileid"), SOCIAL_SCORE("socialscore"), 
	SOCIAL_SCORE_REASON("socialscorereason"),EVENTREQUIRED("eventrequired"), DATE("date");

	private String column;

	private Social(String s) {
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
