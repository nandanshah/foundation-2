package com.dla.foundation.model;

public enum SocialProfile {
	id("id"), name("name"), url("url"), username("username"), location(
			"location"), gender("gender"), country("country"), timezone("timezone");

	private String value;

	SocialProfile(String value) {
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
