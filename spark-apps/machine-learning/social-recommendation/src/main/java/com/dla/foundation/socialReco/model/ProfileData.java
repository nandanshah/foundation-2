package com.dla.foundation.socialReco.model;

public enum ProfileData {

	profileid("id"), homeregionid("homeregionid");

	private String value;

	ProfileData(String value) {
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
