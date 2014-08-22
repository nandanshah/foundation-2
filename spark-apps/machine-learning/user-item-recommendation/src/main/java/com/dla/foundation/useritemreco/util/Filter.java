package com.dla.foundation.useritemreco.util;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.dla.foundation.useritemreco.model.ItemSummary;
import com.dla.foundation.useritemreco.model.UserItemSummary;

public class Filter implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4155719850055225877L;

	public static JavaPairRDD<String, String> filterStringPair(
			JavaPairRDD<String, String> rdd) {
		JavaPairRDD<String, String> filteredRdd = rdd
				.filter(new Function<Tuple2<String, String>, Boolean>() {

					private static final long serialVersionUID = -6021748217701617184L;

					public Boolean call(Tuple2<String, String> record)
							throws Exception {
						if (record != null) {
							if (null != record._1 && null != record._2) {
								return true;
							}
						}
						return false;
					}
				});

		return filteredRdd;
	}

	public static JavaPairRDD<String, ItemSummary> filterItemSummary(
			JavaPairRDD<String, ItemSummary> scoreRDD) {
		JavaPairRDD<String, ItemSummary> filteredScoreRDD = scoreRDD
				.filter(new Function<Tuple2<String, ItemSummary>, Boolean>() {

					private static final long serialVersionUID = -6160930803403593402L;

					public Boolean call(Tuple2<String, ItemSummary> record)
							throws Exception {

						if (record != null) {
							if (null != record._1 && null != record._2) {
								return true;
							}
						}
						return false;
					}
				});

		return filteredScoreRDD;
	}

	public static JavaPairRDD<String, UserItemSummary> filterScoreSummary(
			JavaPairRDD<String, UserItemSummary> javaRDD) {

		JavaPairRDD<String, UserItemSummary> filteredScoreSummaryRDD = javaRDD
				.filter(new Function<Tuple2<String, UserItemSummary>, Boolean>() {

					private static final long serialVersionUID = 4834345712959347417L;

					public Boolean call(Tuple2<String, UserItemSummary> record)
							throws Exception {
						if (record != null) {
							if (record._1 != null && record._2 != null) {
								UserItemSummary userItem = record._2;
								if (userItem.getItemSummary() != null) {
									return true;
								}
							}
						}
						return false;

					}

				});

		return filteredScoreSummaryRDD;
	}

	public static JavaPairRDD<String, UserItemSummary> filterSocial(
			JavaPairRDD<String, UserItemSummary> javaRDD) {

		JavaPairRDD<String, UserItemSummary> filteredSocialSummaryRDD = javaRDD
				.filter(new Function<Tuple2<String, UserItemSummary>, Boolean>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, UserItemSummary> record)
							throws Exception {
						if (record != null) {
							if (record._1 != null && record._2 != null) {
								UserItemSummary userItem = record._2;
								if (userItem.getItemSummary() != null) {
									return true;
								}

							}
						}
						return false;
					}

				});
		return filteredSocialSummaryRDD;
	}

}
