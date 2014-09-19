package com.dla.foundation.trendReco.util;

import java.io.Serializable;

public class TrendRecoProp implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public final static String TREND_RECO_APP_NAME = "TrendRecommendation";

	public final static String USER_EVENT_SUM_APP_NAME = "userevent";
	public final static String USER_EVENT_SUM_INP_CF = "userevent";
	public final static String USER_EVENT_SUM_OUT_CF = "common_daily_eventsummary_per_useritem";

	public final static String DAILY_EVENT_SUMMARY_APP_NAME = "dailyeventsummary";
	public final static String DAY_SCORE_INP_CF = "common_daily_eventsummary_per_useritem";
	public final static String DAY_SCORE_OUT_CF = "trend_daily_eventsummary";

	public final static String TREND_SCORE_FULLY_QUALIFIED_CLASS_NAME = "com.dla.foundation.trendReco.services.ZScoreService";

	public final static String TREND_SCORE_APP_NAME = "trendscore";
	public final static String TREND_SCORE_INP_CF = "trend_daily_eventsummary";
	public final static String TREND_SCORE_OUT_CF = "trend_reco";

	public final static String PARTITIONER = "Murmur3Partitioner";

}
