package com.dla.foundation.socialReco.util;

public enum PropKeys {
	
	//INPUT_DATE("input_date"),
	SOCIAL_RECO_DATE("social_reco_date"),RECAL_START_DATE(
			"recalculation_start_date"), RECAL_END_DATE(
			"recalculation_end_date"),  SOCIAL_RECO_DATE_FLAG(
			"social_reco_date_flag"), FRESHNESS_BUCKET_FOR_LAST_DAY("freshness_bucket_for_last_day"),
			FRESHNESS_BUCKET_FOR_LAST_WEEK("freshness_bucket_for_last_week"), 
			FRESHNESS_BUCKET_FOR_LAST_MONTH("freshness_bucket_for_last_month"),
			TOP_ENTRIES_COUNT("top_entries_cnt"), NUMBER_OF_DAYS("number_of_days"),
			NUMBER_OF_DAYS_FOR_WEEKS("number_of_days_for_weeks"), NUMBER_OF_DAYS_FOR_MONTHS("number_of_days_for_months"),
			CONSIDER_SELF_RECO("consider_self_reco");

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
