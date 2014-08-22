package com.dla.foundation.useritemreco.util;

public enum PropKeys {

	INPUT_DATE(
			"input_date");
	/*MODE_PROPERTY("spark_mode"), APP_NAME("app_name"), INPUT_KEYSPACE(
			"input_keyspace"), INPUT_COLUMNFAMILY("input_columnfamily"), INPUT_PARTITIONER(
			"input_partitioner"), INPUT_RPC_PORT("rpc_port"), OUTPUT_KEYSPACE(
			"output_keyspace"), OUTPUT_COLUMNFAMILY("output_columnfamily"), OUTPUT_PARTITIONER(
			"output_partitioner"), PAGE_ROW_SIZE("page_row_size"), , INPUT_HOST_LIST("read_host_list"), OUTPUT_HOST_LIST(
			"write_host_list"), ITEM_LEVEL_RECO_COLUMNFAMILIES(
			"item_level_columnfamilies"), ITEM_LEVEL_CF_KEYSPACE(
			"item_level_keyspace"), TREND("item_level_trend_columnfamily"), POPULARITY(
			"item_level_popularity_columnfamily"), FP("item_level_fnp_columnfamily"),
			NEW_RELEASE("item_level_new_release_columnfamily"),ITEM_LEVEL_CF_PAGE_ROW_SIZE(
			"item_level_cf_page_row_size"), SCORE_SUMMARY(
			"item_level_score_summary_columnfamily"), ACCOUNT(
			"account_columnfamily"), REGION_ID("item_level_region_id"), TENANT_ID(
			"item_level_tenant_id"), PREFFRED_REGION_ID(
			"profile_level_preferred_region_id"), SOCIAL_RECOMMENDATION(
			"user_level_social_recommendation"), PIO_RECOMMENDATION(
			"user_level_pio_recommendation");*/

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
