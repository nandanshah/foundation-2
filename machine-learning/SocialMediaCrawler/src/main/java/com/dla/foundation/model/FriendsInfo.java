package com.dla.foundation.model;

public enum FriendsInfo {
	userfrdpair("userfrdpair"), relation("relation ");

	private String value;

	FriendsInfo(String value) {
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
