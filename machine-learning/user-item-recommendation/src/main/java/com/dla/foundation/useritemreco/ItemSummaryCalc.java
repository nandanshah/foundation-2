package com.dla.foundation.useritemreco;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.useritemreco.model.ItemSummary;
import com.dla.foundation.useritemreco.model.Score;
import com.dla.foundation.useritemreco.model.ScoreType;
import com.dla.foundation.useritemreco.util.Filter;
import com.dla.foundation.useritemreco.util.ItemSummaryTransformation;
import com.dla.foundation.useritemreco.util.PropKeys;
import com.dla.foundation.useritemreco.util.UserItemRecommendationUtil;
import com.google.common.base.Optional;

/**
 * This class will provide the functionality of performing left join with item
 * level column families.
 * 
 * @author shishir_shivhare
 * 
 */
public class ItemSummaryCalc implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3297305915745901809L;
	private static final String NOT_AVAILABLE = "NA";
	private static final String TRENDING = "trending";
	private static final String DELIMITER = "#";
	private static final String POPULARITY = "popularity";
	private String itemLevelCFKeyspace;
	private Map<String, String> itemLevelRecommendationCF;
	private String pageRowSize;
	private Date inputDate;
	private static final Logger logger = Logger
			.getLogger(ItemSummaryCalc.class);

	public ItemSummaryCalc(String itemLevelCFKeyspace,
			Map<String, String> itemLevelColumnFamilies, String pageRowSize,
			Date inputDate) {
		super();
		this.itemLevelCFKeyspace = itemLevelCFKeyspace;
		this.itemLevelRecommendationCF = itemLevelColumnFamilies;
		this.pageRowSize = pageRowSize;
		this.inputDate = UserItemRecommendationUtil.processInputDate(inputDate);
	}

	/**
	 * This function will provide the functionality of performing left join with
	 * item level column families.
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param itemRDD
	 * @return
	 * @throws Exception
	 */
	public JavaRDD<ItemSummary> calculateScoreSummary(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, String> itemRDD) throws Exception {
		logger.info("performing left outer join between item and trend column family");
		JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> itemTrendRDD = joinTrend(
				sparkContext, cassandraSparkConnector, itemRDD);
		logger.info("performing left outer join between result (left outer join of item and trend) and popularity column family");
		JavaPairRDD<String, Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>> itemTrendPopRDD = joinPopularity(
				sparkContext, cassandraSparkConnector, itemTrendRDD);
		return getScoreSummary(itemTrendPopRDD);
	}

	/**
	 * This function will combine the result of all the join into item summary.
	 * 
	 * @param itemTrendPopRDD
	 * @return
	 */
	private JavaRDD<ItemSummary> getScoreSummary(
			JavaPairRDD<String, Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>> itemTrendPopRDD) {

		logger.info("combining the result of all the joins into item summary");
		JavaRDD<ItemSummary> scoreSummaryRDD = itemTrendPopRDD
				.map(new Function<Tuple2<String, Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>>, ItemSummary>() {

					/**
			 * 
			 */
					private static final long serialVersionUID = -281276554955631663L;
					Score score;
					ItemSummary itemSummary;
					Map<String, Score> scores;

					public ItemSummary call(
							Tuple2<String, Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>> record)
							throws Exception {
						String primaryKeys = record._1;
						String[] keys = primaryKeys.split(DELIMITER);
						scores = new HashMap<String, Score>();
						if (record._2._1._2.isPresent()) {

							scores.put(
									record._2._1._2
											.get()
											.getScores()
											.get(ScoreType.TREND_TYPE
													.getColumn()).getType(),
									record._2._1._2
											.get()
											.getScores()
											.get(ScoreType.TREND_TYPE
													.getColumn()));
						} else {

							score = new Score();
							score.setScore(0.0);
							score.setType(TRENDING);
							score.setScoreReason(NOT_AVAILABLE);
							scores.put(ScoreType.TREND_TYPE.getColumn(), score);
						}
						if (record._2._2.isPresent()) {
							scores.put(
									record._2._2
											.get()
											.getScores()
											.get(ScoreType.POPULARITY_TYPE
													.getColumn()).getType(),
									record._2._2
											.get()
											.getScores()
											.get(ScoreType.POPULARITY_TYPE
													.getColumn()));
						} else {
							score = new Score();
							score.setScore(0.0);
							score.setType(POPULARITY);
							score.setScoreReason(NOT_AVAILABLE);
							scores.put(ScoreType.POPULARITY_TYPE.getColumn(),
									score);
						}
						itemSummary = new ItemSummary(keys[0], keys[1],
								keys[2], scores, inputDate);
						return itemSummary;
					}
				});
		return scoreSummaryRDD;
	}

	/**
	 * This function will fetch the trend column family ,transform and filter it
	 * and then perform left outer join with item column family
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param itemRDD
	 * @return
	 * @throws Exception
	 */
	private JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> joinTrend(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, String> itemRDD) throws Exception {
		String trendCF = itemLevelRecommendationCF.get(PropKeys.TREND
				.getValue());
		if (null != trendCF && "" != trendCF) {
			Configuration conf = new Configuration();
			logger.info("reading trend column family");
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraTrendRDD = cassandraSparkConnector
					.read(conf, sparkContext, itemLevelCFKeyspace, trendCF,
							pageRowSize, UserItemRecommendationUtil
									.getWhereClause(inputDate));
			logger.info("transforming record of trend column family");
			JavaPairRDD<String, ItemSummary> trendRDD = ItemSummaryTransformation
					.getItemSummary(cassandraTrendRDD);
			logger.info("filtering record of trend column family");
			JavaPairRDD<String, ItemSummary> filteredTrendRDD = Filter
					.filterItemSummary(trendRDD);
			logger.info("performing left outer join between item and trend column family");
			//key for both is: tenantid#regionid#itemid
			return itemRDD.leftOuterJoin(filteredTrendRDD);
		} else {
			throw new Exception(
					"Trend Column family name is not properly specified in property file");
		}

	}

	/**
	 * This function will fetch the popularity column family ,transform and
	 * filter it and then perform left outer join with provided the
	 * parameter(result of left outer join of item and trend) column family
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param itemTrendRDD
	 * @return
	 * @throws Exception
	 */
	private JavaPairRDD<String, Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>> joinPopularity(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> itemTrendRDD)
			throws Exception {
		String popularityCF = itemLevelRecommendationCF.get(PropKeys.POPULARITY
				.getValue());

		if (null != popularityCF && "" != popularityCF) {
			Configuration conf = new Configuration();
			logger.info("reading popularity column family");
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraPopularityRDD = cassandraSparkConnector
					.read(conf, sparkContext, itemLevelCFKeyspace,
							popularityCF, pageRowSize,
							UserItemRecommendationUtil
									.getWhereClause(inputDate));
			logger.info("transforming record of popularity column family");
			JavaPairRDD<String, ItemSummary> popularityRDD = ItemSummaryTransformation
					.getItemSummary(cassandraPopularityRDD);
			logger.info("filtering record of popularity column family");
			JavaPairRDD<String, ItemSummary> filteredPopularityRDD = Filter
					.filterItemSummary(popularityRDD);
			logger.info("performing left outer join between result of item and trend with popularity column family");
			//key for both is: tenantid#regionid#itemid
			return itemTrendRDD.leftOuterJoin(filteredPopularityRDD);
		} else {
			throw new Exception(
					"Popularity column family name is not properly specified in property file");
		}
	}

}
