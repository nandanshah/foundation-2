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
import com.dla.foundation.useritemreco.util.UserItemRecoProp;
import com.dla.foundation.useritemreco.util.UserItemRecommendationUtil;
import com.google.common.base.Optional;

/**
 * This class will provide the functionality of performing join with item level
 * column families.
 * 
 * @author shishir_shivhare
 * 
 */
public class ItemSummaryCalc implements Serializable {

	private static final long serialVersionUID = 3297305915745901809L;
	private static final String NOT_AVAILABLE = "NA";
	private static final String DELIMITER = "#";
	private String itemLevelCFKeyspace;
	private String pageRowSize;
	private Date inputDate;
	private static final Logger logger = Logger
			.getLogger(ItemSummaryCalc.class);

	public ItemSummaryCalc(String itemLevelCFKeyspace, String pageRowSize,
			Date inputDate) {
		super();
		this.itemLevelCFKeyspace = itemLevelCFKeyspace;
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
	 */
	public JavaRDD<ItemSummary> calculateScoreSummary(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, String> itemRDD) {
		logger.info("performing left outer join between item and trend column family");
		JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> itemTrendRDD = joinTrend(
				sparkContext, cassandraSparkConnector, itemRDD);
		logger.info("performing left outer join between result (left outer join of item and trend) and popularity column family");
		JavaPairRDD<String, Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>> itemTrendPopRDD = joinPopularity(
				sparkContext, cassandraSparkConnector, itemTrendRDD);
		logger.info("performing left outer join between result (left outer join of item trend and popularity) "
				+ "and new release column family");
		JavaPairRDD<String, Tuple2<Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>, Optional<ItemSummary>>> itemTrendPopNewReleaseRDD = joinNewRelease(
				sparkContext, cassandraSparkConnector, itemTrendPopRDD);

		return getScoreSummary(itemTrendPopNewReleaseRDD);
	}

	/**
	 * This function will combine the result of all the join into item summary.
	 * 
	 * @param itemTrendPopNewReleaseRDD
	 * @return
	 */
	private JavaRDD<ItemSummary> getScoreSummary(
			JavaPairRDD<String, Tuple2<Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>, Optional<ItemSummary>>> itemTrendPopNewReleaseRDD) {

		logger.info("combining the result of all the joins into item summary");
		JavaRDD<ItemSummary> scoreSummaryRDD = itemTrendPopNewReleaseRDD
				.map(new Function<Tuple2<String, Tuple2<Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>, Optional<ItemSummary>>>, ItemSummary>() {

					private static final long serialVersionUID = -281276554955631663L;
					Score score;
					ItemSummary itemSummary;
					Map<String, Score> scores;

					public ItemSummary call(
							Tuple2<String, Tuple2<Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>, Optional<ItemSummary>>> record) {
						String primaryKeys = record._1;
						String[] keys = primaryKeys.split(DELIMITER);
						scores = new HashMap<String, Score>();
						if (record._2._1._1._2.isPresent()) {

							scores.put(
									record._2._1._1._2
											.get()
											.getScores()
											.get(ScoreType.TREND_TYPE
													.getColumn()).getType(),
									record._2._1._1._2
											.get()
											.getScores()
											.get(ScoreType.TREND_TYPE
													.getColumn()));
						} else {

							score = new Score();
							score.setScore(0.0);
							score.setType(ScoreType.TREND_TYPE.getColumn());
							score.setScoreReason(NOT_AVAILABLE);
							scores.put(ScoreType.TREND_TYPE.getColumn(), score);
						}
						if (record._2._1._2.isPresent()) {
							scores.put(
									record._2._1._2
											.get()
											.getScores()
											.get(ScoreType.POPULARITY_TYPE
													.getColumn()).getType(),
									record._2._1._2
											.get()
											.getScores()
											.get(ScoreType.POPULARITY_TYPE
													.getColumn()));
						} else {
							score = new Score();
							score.setScore(0.0);
							score.setType(ScoreType.POPULARITY_TYPE.getColumn());
							score.setScoreReason(NOT_AVAILABLE);
							scores.put(ScoreType.POPULARITY_TYPE.getColumn(),
									score);
						}
						if (record._2._2.isPresent()) {
							scores.put(
									record._2._2
											.get()
											.getScores()
											.get(ScoreType.NEW_RELEASE_TYPE
													.getColumn()).getType(),
									record._2._2
											.get()
											.getScores()
											.get(ScoreType.NEW_RELEASE_TYPE
													.getColumn()));
						} else {
							score = new Score();
							score.setScore(0.0);
							score.setType(ScoreType.NEW_RELEASE_TYPE
									.getColumn());
							score.setScoreReason(NOT_AVAILABLE);
							scores.put(ScoreType.NEW_RELEASE_TYPE.getColumn(),
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
	 */
	private JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> joinTrend(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, String> itemRDD) {
		String trendCF = UserItemRecoProp.ITEM_LEVEL_TREND_CF;

		Configuration conf = new Configuration();
		logger.info("reading trend column family");
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraTrendRDD = cassandraSparkConnector
				.read(conf, sparkContext, itemLevelCFKeyspace, trendCF,
						pageRowSize,
						UserItemRecommendationUtil.getWhereClause(inputDate));
		logger.info("transforming record of trend column family");
		JavaPairRDD<String, ItemSummary> trendRDD = ItemSummaryTransformation
				.getItemSummary(cassandraTrendRDD);
		logger.info("filtering record of trend column family");
		JavaPairRDD<String, ItemSummary> filteredTrendRDD = Filter
				.filterItemSummary(trendRDD);
		logger.info("performing left outer join between item and trend column family");
		// key for both is: tenantid#regionid#itemid
		return itemRDD.leftOuterJoin(filteredTrendRDD);

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
	 */
	private JavaPairRDD<String, Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>> joinPopularity(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> itemTrendRDD) {
		String popularityCF = UserItemRecoProp.ITEM_LEVEL_POPULARITY_CF;

		Configuration conf = new Configuration();
		logger.info("reading popularity column family");
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraPopularityRDD = cassandraSparkConnector
				.read(conf, sparkContext, itemLevelCFKeyspace, popularityCF,
						pageRowSize,
						UserItemRecommendationUtil.getWhereClause(inputDate));
		logger.info("transforming record of popularity column family");
		JavaPairRDD<String, ItemSummary> popularityRDD = ItemSummaryTransformation
				.getItemSummary(cassandraPopularityRDD);
		logger.info("filtering record of popularity column family");
		JavaPairRDD<String, ItemSummary> filteredPopularityRDD = Filter
				.filterItemSummary(popularityRDD);
		logger.info("performing left outer join between result of item and trend with popularity column family");
		// key for both is: tenantid#regionid#itemid
		return itemTrendRDD.leftOuterJoin(filteredPopularityRDD);
	}

	/**
	 * This function will fetch the New Release column family ,transform and
	 * filter it and then perform left outer join with provided the
	 * parameter(result of left outer join of item,trend and popularity) column
	 * family
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param itemTrendPopRDD
	 * @return
	 */
	private JavaPairRDD<String, Tuple2<Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>, Optional<ItemSummary>>> joinNewRelease(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, Tuple2<Tuple2<String, Optional<ItemSummary>>, Optional<ItemSummary>>> itemTrendPopRDD) {
		String newReleaseCF = UserItemRecoProp.ITEM_LEVEL_NEW_RELEASE_CF;

		Configuration conf = new Configuration();
		logger.info("reading newReleaseCF column family");
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraFpRDD = cassandraSparkConnector
				.read(conf, sparkContext, itemLevelCFKeyspace, newReleaseCF,
						pageRowSize,
						UserItemRecommendationUtil.getWhereClause(inputDate));
		logger.info("transforming record of new release column family");
		JavaPairRDD<String, ItemSummary> newReleaseRDD = ItemSummaryTransformation
				.getItemSummary(cassandraFpRDD);
		logger.info("filtering record of new release column family");
		JavaPairRDD<String, ItemSummary> filteredNewReleaseRDD = Filter
				.filterItemSummary(newReleaseRDD);
		logger.info("performing left outer join between result of item trend and popularity with new release column family");
		// key for both is: tenantid#regionid#itemid
		return itemTrendPopRDD.leftOuterJoin(filteredNewReleaseRDD);

	}
}
