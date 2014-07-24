package com.dla.foundation.useritemreco.services;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.useritemreco.model.ItemSummary;
import com.dla.foundation.useritemreco.model.UserItemSummary;
import com.dla.foundation.useritemreco.util.Filter;
import com.dla.foundation.useritemreco.util.ScoreSummaryTransformation;
import com.dla.foundation.useritemreco.util.UserItemRecommendationUtil;
import com.google.common.base.Optional;

public class UserItemSummaryService implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3133298652347309107L;
	private String itemLevelCFKeyspace;
	private String scoreSummaryCF;
	private String pageRowSize;
	private Date inputDate;

	public UserItemSummaryService(String itemLevelCFKeyspace,
			String scoreSummaryCF, String pageRowSize, Date inputDate) {
		super();
		this.itemLevelCFKeyspace = itemLevelCFKeyspace;
		this.scoreSummaryCF = scoreSummaryCF;
		this.pageRowSize = pageRowSize;
		this.inputDate = inputDate;
	}

	public JavaRDD<UserItemSummary> calculateUserItemSummary(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, String> profileRDD) throws Exception {
		JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> profileItemRDD = joinItemScoreSummary(
				sparkContext, cassandraSparkConnector, profileRDD);
		JavaRDD<UserItemSummary> filteredUserItemSummaryRDD = Filter
				.filterScoreSummary(getUserItemSummary(profileItemRDD));
		return filteredUserItemSummaryRDD;

	}

	private JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> joinItemScoreSummary(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, String> profileRDD) throws Exception {
		if (null != scoreSummaryCF && "" != scoreSummaryCF) {
			Configuration conf = new Configuration();
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraScoreSummaryRDD = cassandraSparkConnector
					.read(conf, sparkContext, itemLevelCFKeyspace,
							scoreSummaryCF, pageRowSize,
							UserItemRecommendationUtil
									.getWhereClause(inputDate));
			JavaPairRDD<String, ItemSummary> scoreSummaryRDD = ScoreSummaryTransformation
					.getScoreSummary(cassandraScoreSummaryRDD);
			JavaPairRDD<String, ItemSummary> filteredScoreSummaryRDD = Filter
					.filterItemSummary(scoreSummaryRDD);
			return profileRDD.leftOuterJoin(filteredScoreSummaryRDD);

		} else {
			throw new Exception(
					"column family name for score summary is not proper");
		}
	}

	private JavaRDD<UserItemSummary> getUserItemSummary(
			JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> profileItemRDD) {
		JavaRDD<UserItemSummary> userItemSummaryRDD = profileItemRDD
				.map(new Function<Tuple2<String, Tuple2<String, Optional<ItemSummary>>>, UserItemSummary>() {
					/**
		 * 
		 */
					private static final long serialVersionUID = 7819779796058281951L;
					UserItemSummary userItemSummary;

					public UserItemSummary call(
							Tuple2<String, Tuple2<String, Optional<ItemSummary>>> record)
							throws Exception {
						String userId = record._2._1;

						if (record._2._2.isPresent()) {
							userItemSummary = new UserItemSummary(userId,
									record._2._2.get());
							return userItemSummary;
						}

						return null;
					}
				});

		return userItemSummaryRDD;
	}

}
