package com.dla.foundation.trendReco.services;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.dla.foundation.trendReco.model.DayScore;
import com.dla.foundation.trendReco.model.TrendScore;

/**
 * This interface has to be implemented by any service which is going to provide
 * functionality of calculating trend score. It should also provide the
 * functionality of calculating normalized score for their trend score.
 * 
 * @author shishir_shivhare
 * 
 */
public interface ITrendScore {
	public JavaRDD<TrendScore> calculateScore(
			JavaPairRDD<String, DayScore> itemDayScoreRDD);

	public JavaRDD<TrendScore> normalizedScore(
			JavaRDD<TrendScore> itemTrendScoreRDD);
}
