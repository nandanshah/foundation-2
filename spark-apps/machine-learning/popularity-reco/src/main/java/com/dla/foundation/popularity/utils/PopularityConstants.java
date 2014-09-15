package com.dla.foundation.popularity.utils;

import java.io.Serializable;

public class PopularityConstants implements Serializable{
	public static final String APP_NAME = "popularityReco";
	public static final String LAST_EXECUTION = "last_execution";
	public static final String EVENT_SUMMARY_CF = "trend_daily_eventsummary";
	public static final String POPULARITY_CF = "popularity_reco";
	public static final String FULLCOMPUTE = "fullcompute";
	public static final String INCREMENTAL = "incremental";
	public static final String INPUT_DATE = "date";
	public static final String INPUT_START_DATE = "fullcompute_start_date";
	public static final String INPUT_END_DATE = "fullcompute_end_date";
	public static final String INPUT_PARTITIONER = "Murmur3Partitioner";
	public static final String OUTPUT_PARTITIONER = "Murmur3Partitioner";
	public static final String DATE_FORMAT = "yyyy-MM-dd";
	public static final int EVENT_REQUIRED = 1;
	public static final String RECO_REASON = "Score by Machine Learning";
	
	

}