package com.dla.foundation.useritemreco.util;

public enum PropKeys {

	INPUT_DATE("input_date");

	private String value;

	PropKeys(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return value;
	}

}
