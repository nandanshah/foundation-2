package com.dla.foundation.useritemreco.util;

import java.io.Serializable;

/*
 * This class provides all the static properties required by the useritemreco app
 * 
 */
public class UserItemRecoProp implements Serializable {

	private static final long serialVersionUID = 464427147528049305L;

	public final static String USER_ITEM_RECO_APP_NAME = "useritemreco";

	public final static String INPUT_CF_ITEM = "itemdocument";
	public final static String OUTPUT_CF_ITEM = "user_item_score_summary";
	public final static String ITEM_LEVEL_TREND_CF = "trend_reco";
	public final static String ITEM_LEVEL_REGION_ID = "regionid";
	public final static String ITEM_LEVEL_TENANT_ID = "tenantid";
	public final static String ITEM_LEVEL_POPULARITY_CF = "popularity_reco";
	public final static String ITEM_LEVEL_FNP_CF = "fnp_reco";
	public final static String ITEM_LEVEL_NEW_RELEASE_CF = "new_release_reco";

	public final static String INPUT_CF_PROFILE = "profile";
	public final static String OUTPUT_CF = "user_item_reco";
	public final static String PROFILE_LEVEL_PREFERRED_REGION_ID = "homeregionid";
	public final static String ITEM_LEVEL_SCORE_SUMMARY_CF = "user_item_score_summary";
	public final static String USER_LEVEL_SOCIAL_RECOMMENDATION = "social_reco";
	public final static String USER_LEVEL_PIO_RECOMMENDATION = "pio_user_reco";
	public final static String ACCOUNT_CF = "account";

	public final static String PARTITIONER = "Murmur3Partitioner";
}
