package com.dla.foundation.socialReco.util;

import java.io.Serializable;

public class SocialRecoProp implements Serializable{
	
	public final static String SOCIAL_RECO_APP_NAME = "SocialRecommendation";

	public final static String USER_EVENT_SUM_INP_CF = "common_daily_eventsummary_per_useritem";
	public final static String PROFILE_INP_CF = "profile";
	public final static String FRIENDSINFO_INP_CF = "social_friends_info";
	
	public final static String SOCIAL_SCORE_APP_NAME = "SocialScore";
	public final static String SOCIAL_SCORE_OUT_CF = "social_reco";

	public final static String PARTITIONER = "Murmur3Partitioner";
}
