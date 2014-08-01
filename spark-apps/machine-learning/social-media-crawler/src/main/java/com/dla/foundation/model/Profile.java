package com.dla.foundation.model;

public enum Profile {

	id("id"), socialauth("socialauth"), lastmodified("lastmodified"), dummyflag("dummyflag");

	private String value;

	Profile(String value) {
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
