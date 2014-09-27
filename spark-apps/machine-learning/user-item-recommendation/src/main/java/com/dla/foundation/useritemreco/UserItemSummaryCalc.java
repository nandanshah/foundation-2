package com.dla.foundation.useritemreco;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.utils.UUIDGen;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.useritemreco.model.ItemSummary;
import com.dla.foundation.useritemreco.model.Score;
import com.dla.foundation.useritemreco.model.ScoreType;
import com.dla.foundation.useritemreco.model.UserItemSummary;
import com.dla.foundation.useritemreco.util.Filter;
import com.dla.foundation.useritemreco.util.RecoTransformations;
import com.dla.foundation.useritemreco.util.ScoreSummaryTransformation;
import com.dla.foundation.useritemreco.util.UserItemRecoProp;
import com.dla.foundation.useritemreco.util.UserItemRecommendationUtil;
import com.google.common.base.Optional;

/**
 * This class is used to convert item level summary to user item level summary
 * by performing left outer between profile and score summary and performs left
 * join with social reco and pio reco.
 * 
 * @author shishir_shivhare
 * 
 */
public class UserItemSummaryCalc implements Serializable {

	private static final long serialVersionUID = -3133298652347309107L;
	private static final String DELIMITER_PROPERTY = "#";
	private static final String NOT_AVAILABLE = "NA";
	private String itemLevelCFKeyspace;
	private String scoreSummaryCF;
	private String pageRowSize;
	private Date inputDate;

	String socialCF;
	String pioCF;

	private static final Logger logger = Logger
			.getLogger(UserItemSummaryCalc.class);

	public UserItemSummaryCalc(String itemLevelCFKeyspace,
			String scoreSummaryCF, String pageRowSize, Date inputDate) {
		super();
		this.itemLevelCFKeyspace = itemLevelCFKeyspace;
		this.scoreSummaryCF = scoreSummaryCF;
		this.pageRowSize = pageRowSize;
		this.inputDate = inputDate;

	}

	/**
	 * 
	 * This function is used to convert item level summary to user item level
	 * summary by performing left outer between profile and score summary and
	 * also provide the functionality of performing left join with social reco
	 * and pio reco.
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param profileRDD
	 * @return
	 */
	public JavaRDD<UserItemSummary> calculateUserItemSummary(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, String> profileRDD) {

		logger.info("peforming left outer join between profile and score summary");
		JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> profileItemRDD = joinItemScoreSummary(
				sparkContext, cassandraSparkConnector, profileRDD);

		JavaPairRDD<String, UserItemSummary> userItemSummaryInfo = getUserItemSummary(profileItemRDD);

		JavaPairRDD<String, UserItemSummary> filteredUserItem = Filter
				.filterScoreSummary(userItemSummaryInfo);

		logger.info("performing left outer join between profile/Score-summary and social");
		JavaPairRDD<String, Tuple2<UserItemSummary, Optional<UserItemSummary>>> joinedSocial = getUserItemSocialSummary(
				cassandraSparkConnector, sparkContext, filteredUserItem);

		logger.info("performing left outer join between profile/Score-summary/social & Pio ");
		JavaPairRDD<String, Tuple2<Tuple2<UserItemSummary, Optional<UserItemSummary>>, Optional<UserItemSummary>>> pioCombined = getPioSummary(
				cassandraSparkConnector, sparkContext, joinedSocial);

		logger.info("combining all the scores");
		JavaPairRDD<String, UserItemSummary> filteredUserItemScorePio = comibneUserItemSocialPio(pioCombined);

		JavaRDD<UserItemSummary> filteredUserItemSummaryRDD = filteredUserItemScorePio
				.values();

		JavaRDD<UserItemSummary> UserItemSummaryRDD = getJustification(filteredUserItemSummaryRDD);

		UserItemSummaryRDD.cache();

		JavaRDD<UserItemSummary> defaultUserDetails = addDefaultUser(UserItemSummaryRDD);
		JavaRDD<UserItemSummary> UserItemDetails = UserItemSummaryRDD
				.union(defaultUserDetails);
		return UserItemDetails;

	}

	private JavaRDD<UserItemSummary> getJustification(
			JavaRDD<UserItemSummary> filterdUserItemRDD) {
		JavaRDD<UserItemSummary> justificationRDD = filterdUserItemRDD
				.map(new Function<UserItemSummary, UserItemSummary>() {

					private static final long serialVersionUID = -382661029200011284L;
					UserItemSummary userItem;
					Map<String, Score> scoreMap;
					String reason = "";
					double maxScore = 0.0;

					Set<String> keys;

					@Override
					public UserItemSummary call(UserItemSummary record) {
						userItem = new UserItemSummary();
						userItem = record;
						scoreMap = new HashMap<>();
						scoreMap = userItem.getItemSummary().getScores();
						/*
						 * trendScore =
						 * scoreMap.get(ScoreType.TREND_TYPE).getScore();
						 * popularityScore =
						 * scoreMap.get(ScoreType.POPULARITY_TYPE).getScore();
						 * FPScore = scoreMap.get(ScoreType.FP_TYPE).getScore();
						 * newReleaseScore =
						 * scoreMap.get(ScoreType.NEW_RELEASE_TYPE).getScore();
						 * PIOScore =
						 * scoreMap.get(ScoreType.PIO_TYPE).getScore();
						 * socialScore
						 */
						keys = new HashSet<>();
						keys = scoreMap.keySet();

						for (String values : keys) {
							if (scoreMap.get(values).getScore() > maxScore) {
								maxScore = scoreMap.get(values).getScore();
								reason = scoreMap.get(values).getScoreReason();
							}

						}

						userItem.setJustification(reason);
						return userItem;

					}
				});
		return justificationRDD;
	}

	/**
	 * This function will fetch the PIO column family ,transform and filter it
	 * and then perform left outer join with provided the parameter(result of
	 * left outer join of scoresummary,profile and social) column family
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param joinedSocial
	 * @return
	 */
	private JavaPairRDD<String, Tuple2<Tuple2<UserItemSummary, Optional<UserItemSummary>>, Optional<UserItemSummary>>> getPioSummary(
			CassandraSparkConnector cassandraSparkConnector,
			JavaSparkContext sparkContext,
			JavaPairRDD<String, Tuple2<UserItemSummary, Optional<UserItemSummary>>> joinedSocial) {
		pioCF = UserItemRecoProp.USER_LEVEL_PIO_RECOMMENDATION;
		Configuration pioConf = new Configuration();
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraPioRDD = cassandraSparkConnector
				.read(pioConf, sparkContext, itemLevelCFKeyspace, pioCF,
						pageRowSize, UserItemRecommendationUtil.getWhereClause(
								inputDate, pioCF));
		logger.info("transformaing PIO column family");

		JavaPairRDD<String, UserItemSummary> pioRDD = RecoTransformations
				.getTransformations(cassandraPioRDD);
		logger.debug("filtering PIO");
		JavaPairRDD<String, UserItemSummary> filteredPioRDD = Filter
				.userRecos(pioRDD);
		logger.debug("performing left outer join between ScoreSummary, profile,Social & PIO");
		JavaPairRDD<String, Tuple2<Tuple2<UserItemSummary, Optional<UserItemSummary>>, Optional<UserItemSummary>>> poiCombined = joinedSocial
				.leftOuterJoin(filteredPioRDD);

		return poiCombined;
	}

	/**
	 * This function will fetch the social column family ,transform and filter
	 * it and then perform left outer join with provided the parameter(result of
	 * left outer join of scoresummary & profile) column family
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param filteredUserItem
	 * @return
	 */
	private JavaPairRDD<String, Tuple2<UserItemSummary, Optional<UserItemSummary>>> getUserItemSocialSummary(
			CassandraSparkConnector cassandraSparkConnector,
			JavaSparkContext sparkContext,
			JavaPairRDD<String, UserItemSummary> filteredUserItem) {
		socialCF = UserItemRecoProp.USER_LEVEL_SOCIAL_RECOMMENDATION;

		Configuration socialConf = new Configuration();
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraSocialRDD = cassandraSparkConnector
				.read(socialConf, sparkContext, itemLevelCFKeyspace, socialCF,
						pageRowSize, UserItemRecommendationUtil.getWhereClause(
								inputDate, socialCF));

		logger.info("transforming social column family");
		JavaPairRDD<String, UserItemSummary> socialRDD = RecoTransformations
				.getTransformations(cassandraSocialRDD);
		logger.info("filtering social");
		JavaPairRDD<String, UserItemSummary> filteredSocialRDD = Filter
				.userRecos(socialRDD);
		logger.info("performing left outer join between ScoreSummary, profile & Social");
		JavaPairRDD<String, Tuple2<UserItemSummary, Optional<UserItemSummary>>> joinedSocial = filteredUserItem
				.leftOuterJoin(filteredSocialRDD);
		return joinedSocial;
	}

	/**
	 * This function will combine the result of all the join into user-item
	 * summary.
	 * 
	 * @param pioCombined
	 * @return
	 */
	private JavaPairRDD<String, UserItemSummary> comibneUserItemSocialPio(
			JavaPairRDD<String, Tuple2<Tuple2<UserItemSummary, Optional<UserItemSummary>>, Optional<UserItemSummary>>> pioCombined) {
		JavaPairRDD<String, UserItemSummary> filteredUserItem = pioCombined
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<UserItemSummary, Optional<UserItemSummary>>, Optional<UserItemSummary>>>, String, UserItemSummary>() {

					private static final long serialVersionUID = -5526483327835915185L;
					Map<String, Score> pioScores = new HashMap<String, Score>();
					Map<String, Score> socialScores = new HashMap<String, Score>();
					Map<String, Score> combinedScores = new HashMap<String, Score>();
					UserItemSummary combinedUserItem;
					UserItemSummary pioUserItem;
					UserItemSummary socialUserItem;
					ItemSummary combinedItem;
					ItemSummary pioItem;
					ItemSummary socialItem;

					@Override
					public Tuple2<String, UserItemSummary> call(
							Tuple2<String, Tuple2<Tuple2<UserItemSummary, Optional<UserItemSummary>>, Optional<UserItemSummary>>> record) {

						Score socialScoreInfo = new Score();
						Score pioScoreInfo = new Score();
						String primaryKey = record._1;
						combinedUserItem = record._2._1._1;
						combinedItem = combinedUserItem.getItemSummary();
						combinedScores = combinedItem.getScores();
						if (record._2._1._2.isPresent()) {

							socialUserItem = record._2._1._2.get();
							socialItem = socialUserItem.getItemSummary();
							socialScores = socialItem.getScores();

							socialScoreInfo = socialScores
									.get(ScoreType.SOCIAL_TYPE.getColumn());
						} else {
							socialScoreInfo.setType(ScoreType.SOCIAL_TYPE
									.getColumn());
							socialScoreInfo.setScoreReason(NOT_AVAILABLE);
							socialScoreInfo.setScore(0);
						}

						if (record._2._2.isPresent()) {

							pioUserItem = record._2._2.get();
							pioItem = pioUserItem.getItemSummary();
							pioScores = pioItem.getScores();

							pioScoreInfo = pioScores.get(ScoreType.PIO_TYPE
									.getColumn());

						} else {

							pioScoreInfo.setType(ScoreType.PIO_TYPE.getColumn());
							pioScoreInfo.setScoreReason(NOT_AVAILABLE);
							pioScoreInfo.setScore(0);
							pioScores.put(pioScoreInfo.getType(), pioScoreInfo);

						}

						combinedScores.put(socialScoreInfo.getType(),
								socialScoreInfo);
						combinedScores.put(pioScoreInfo.getType(), pioScoreInfo);
						return new Tuple2<String, UserItemSummary>(primaryKey,
								combinedUserItem);
					}
				});

		return filteredUserItem;
	}

	/**
	 * This function will provide the functionality of performing left join of
	 * profile with item level column family(score summary).
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param profileRDD
	 * @return
	 */
	private JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> joinItemScoreSummary(
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			JavaPairRDD<String, String> profileRDD) {
		Configuration conf = new Configuration();
		logger.info("reading from item summary column family");
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraScoreSummaryRDD = cassandraSparkConnector
				.read(conf, sparkContext, itemLevelCFKeyspace, scoreSummaryCF,
						pageRowSize,
						UserItemRecommendationUtil.getWhereClause(inputDate));
		logger.info("transforming item summary column family");
		JavaPairRDD<String, ItemSummary> scoreSummaryRDD = ScoreSummaryTransformation
				.getScoreSummary(cassandraScoreSummaryRDD);

		logger.info("filtering item summary  column family");
		JavaPairRDD<String, ItemSummary> filteredScoreSummaryRDD = Filter
				.filterItemSummary(scoreSummaryRDD);

		logger.info("performing left outer join between profile and item summary  column family");
		return profileRDD.leftOuterJoin(filteredScoreSummaryRDD);

	}

	/**
	 * This function will combine the result of all the join into user item
	 * summary.
	 * 
	 * @param profileItemRDD
	 * @return
	 */
	private JavaPairRDD<String, UserItemSummary> getUserItemSummary(
			JavaPairRDD<String, Tuple2<String, Optional<ItemSummary>>> profileItemRDD) {
		logger.info("combining the result of all the join into user item summary.");
		JavaPairRDD<String, UserItemSummary> userItemSummaryRDD = profileItemRDD
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<ItemSummary>>>, String, UserItemSummary>() {

					private static final long serialVersionUID = 7819779796058281951L;
					ItemSummary itemSummary;
					UserItemSummary userItemSummary;
					String item_id = null;
					String profile_id = null;
					String tenantId = null;
					String region_id = null;
					String primary_key = null;

					@Override
					public Tuple2<String, UserItemSummary> call(
							Tuple2<String, Tuple2<String, Optional<ItemSummary>>> record) {

						if (record._2._2.isPresent()) {
							itemSummary = record._2._2.get();
							if (itemSummary != null) {

								region_id = itemSummary.getRegionId();
								tenantId = itemSummary.getTenantId();
								item_id = itemSummary.getItemId();
								profile_id = record._2._1;
								userItemSummary = new UserItemSummary(
										profile_id, itemSummary);

								primary_key = tenantId + DELIMITER_PROPERTY
										+ region_id + DELIMITER_PROPERTY
										+ item_id + DELIMITER_PROPERTY
										+ profile_id;
								return new Tuple2<String, UserItemSummary>(
										primary_key, userItemSummary);
							}
						}
						return null;

					}
				});

		return userItemSummaryRDD;
	}

	/**
	 * This function return a default user for each tenant-region
	 * 
	 * @param filteredUserItemSummaryRDD
	 * @return
	 */
	private JavaRDD<UserItemSummary> addDefaultUser(
			JavaRDD<UserItemSummary> filteredUserItemSummaryRDD) {

		JavaPairRDD<String, Iterable<UserItemSummary>> mapRegionTenant_to_User = getRegionTenantUserSummary(filteredUserItemSummaryRDD);
		JavaPairRDD<String, UserItemSummary> defaultUsers = getSummaryForTenant_Region(mapRegionTenant_to_User);
		JavaRDD<UserItemSummary> default_users = defaultUsers.values();

		return default_users;

	}

	/**
	 * This function will return a list of users for a particular tenant-region
	 * 
	 * @param filteredUserItemSummaryRDD
	 * @return
	 */
	private JavaPairRDD<String, Iterable<UserItemSummary>> getRegionTenantUserSummary(
			JavaRDD<UserItemSummary> filteredUserItemSummaryRDD) {
		JavaPairRDD<String, UserItemSummary> getTenantRegionId = filteredUserItemSummaryRDD
				.mapToPair(new PairFunction<UserItemSummary, String, UserItemSummary>() {

					private static final long serialVersionUID = -1096538231913701100L;

					@Override
					public Tuple2<String, UserItemSummary> call(
							UserItemSummary record) {

						String primaryKey = null;
						UserItemSummary otherColumns = null;
						primaryKey = record.getItemSummary().getTenantId()
								+ "#" + record.getItemSummary().getRegionId()
								+ "#" + record.getItemSummary().getItemId();
						otherColumns = record;

						return new Tuple2<String, UserItemSummary>(primaryKey,
								otherColumns);
					}
				});

		JavaPairRDD<String, Iterable<UserItemSummary>> mapRegionTenant_to_User = getTenantRegionId
				.distinct().groupByKey();
		return mapRegionTenant_to_User;
	}

	/**
	 * This function returns a single user for a tenant-region
	 * 
	 * @param mapRegionTenant_to_User
	 * @return
	 */
	private JavaPairRDD<String, UserItemSummary> getSummaryForTenant_Region(
			JavaPairRDD<String, Iterable<UserItemSummary>> mapRegionTenant_to_User) {
		JavaPairRDD<String, UserItemSummary> defaultUsers = mapRegionTenant_to_User
				.mapToPair(new PairFunction<Tuple2<String, Iterable<UserItemSummary>>, String, UserItemSummary>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, UserItemSummary> call(
							Tuple2<String, Iterable<UserItemSummary>> record) {
						UserItemSummary otherColumns = null;
						String primaryKey = null;
						primaryKey = record._1;
						int flag = 0;
						for (UserItemSummary user : record._2) {
							if (flag == 0) {
								otherColumns = user;
								otherColumns.setUserId(UUIDGen.maxTimeUUID(
										631152001).toString());

								flag = 1;

							} else {
								break;
							}
						}
						return new Tuple2<String, UserItemSummary>(primaryKey,
								otherColumns);
					}

				});

		return defaultUsers;
	}

}
