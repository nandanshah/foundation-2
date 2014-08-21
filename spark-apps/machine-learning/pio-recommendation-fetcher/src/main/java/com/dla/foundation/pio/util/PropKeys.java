package com.dla.foundation.pio.util;

public enum PropKeys {
	PIO_NUM_REC_PER_USER("numRecsPerUser"), //Number of recommendations to be fetched for each user.
	PIO_RECOMMEND_CF("recommendCF"); // Target Recommendation Table	

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
