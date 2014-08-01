package com.dla.foundation.trendReco.util;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.dla.foundation.trendReco.model.DayScore;
import com.dla.foundation.trendReco.model.UserEvent;
import com.dla.foundation.trendReco.model.UserSummary;

/**
 * This class provides the utility to filter those records which are incomplete.
 * 
 * @author shishir_shivhare
 * 
 */
public class Filter implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4155719850055225877L;

	/**
	 * 
	 * @param userEventRDD
	 * @return Records which are complete.
	 */
	public static JavaPairRDD<String, UserEvent> filterUserEvent(
			JavaPairRDD<String, UserEvent> userEventRDD) {
		JavaPairRDD<String, UserEvent> filteredUserEventRDD = userEventRDD
				.filter(new Function<Tuple2<String, UserEvent>, Boolean>() {

					/**
			 * 
			 */
					private static final long serialVersionUID = -4863594971901836926L;

					@Override
					public Boolean call(Tuple2<String, UserEvent> record)
							throws Exception {
						if (record != null) {
							if (null != record._1 && null != record._2) {
								return true;
							}
						}
						return false;
					}
				});
		return filteredUserEventRDD;
	}

	/**
	 * 
	 * @param userSummarytRDD
	 * @return Records which are complete.
	 */
	public static JavaPairRDD<String, UserSummary> filterUserSummaryEvent(
			JavaPairRDD<String, UserSummary> userSummarytRDD) {
		JavaPairRDD<String, UserSummary> filteredUserEventRDD = userSummarytRDD
				.filter(new Function<Tuple2<String, UserSummary>, Boolean>() {

					/**
			 * 
			 */
					private static final long serialVersionUID = -4863594971901836926L;

					@Override
					public Boolean call(Tuple2<String, UserSummary> record)
							throws Exception {
						if (record != null) {
							if (null != record._1 && null != record._2) {
								return true;
							}
						}
						return false;
					}
				});
		return filteredUserEventRDD;
	}

	/**
	 * 
	 * @param dayScoreRDD
	 * @return Records which are complete.
	 */
	public static JavaPairRDD<String, DayScore> filterDayScoreEvent(
			JavaPairRDD<String, DayScore> dayScoreRDD) {
		JavaPairRDD<String, DayScore> filterDayScoreEventRDD = dayScoreRDD
				.filter(new Function<Tuple2<String, DayScore>, Boolean>() {

					/**
			 * 
			 */
					private static final long serialVersionUID = -4863594971901836926L;

					@Override
					public Boolean call(Tuple2<String, DayScore> record)
							throws Exception {
						if (record != null) {
							if (null != record._1 && null != record._2) {
								return true;
							}
						}
						return false;
					}
				});
		return filterDayScoreEventRDD;
	}
}
