package com.dla.foundation.useritemreco.model;

public enum ScoreType {

	TREND_TYPE("trend"), POPULARITY_TYPE("popularity");

	private String column;

	private ScoreType(String s) {
		column = s;
	}

	public String getColumn() {
		return column;
	}

}
