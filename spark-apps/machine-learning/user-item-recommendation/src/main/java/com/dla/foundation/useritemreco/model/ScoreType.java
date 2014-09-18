package com.dla.foundation.useritemreco.model;

public enum ScoreType {

	TREND_TYPE("trend"), POPULARITY_TYPE("popularity"), FP_TYPE("fp"), NEW_RELEASE_TYPE(
			"newRelease"), SOCIAL_TYPE("social"), PIO_TYPE("pio");

	private String column;

	private ScoreType(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}

}
