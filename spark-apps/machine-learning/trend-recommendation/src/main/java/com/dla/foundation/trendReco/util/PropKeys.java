package com.dla.foundation.trendReco.util;

public enum PropKeys {

	EVENT_REQUIRED("event_required"), INPUT_DATE("input_date"), RECAL_START_DATE(
			"recalculation_start_date"), RECAL_END_DATE(
			"recalculation_end_date"), RECAL_PERIOD("recalculation_period"), INCREMENTAL_FLAG(
			"incremental_flag"), ZSCORE_PERIOD("zscore_period"), CURRENT_TREND_DATE(
			"current_trend_date");

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
