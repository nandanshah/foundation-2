package com.dla.foundation.useritemreco;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.useritemreco.model.CassandraConfig;
import com.dla.foundation.useritemreco.model.ItemSummary;
import com.dla.foundation.useritemreco.model.UserItemSummary;
import com.dla.foundation.useritemreco.model.userItemRecoCF;
import com.dla.foundation.useritemreco.services.ItemSummaryService;
import com.dla.foundation.useritemreco.services.UserItemSummaryService;
import com.dla.foundation.useritemreco.util.AccountTransformation;
import com.dla.foundation.useritemreco.util.Filter;
import com.dla.foundation.useritemreco.util.ItemTransformation;
import com.dla.foundation.useritemreco.util.ProfileTransformation;
import com.dla.foundation.useritemreco.util.PropKeys;
import com.dla.foundation.useritemreco.util.UserItemRecoPostprocessing;
import com.dla.foundation.useritemreco.util.UserItemRecommendationUtil;

public class UserItemRecoDriver implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4424270924188805825L;
	private static final String DATE_FORMAT = "yyyy-MM-dd";

	public void runUserItemRecoDriver(String appPropFilePath,
			String itmSumPropFilePath, String userItemPropFilePath) {
		PropertiesHandler appProp;
		try {
			appProp = new PropertiesHandler(appPropFilePath);
			JavaSparkContext sparkContext = initSparkContext(appProp);
			CassandraSparkConnector cassandraSparkConnector = initCassandraSparkConn(appProp);
			runItemSummary(itmSumPropFilePath, sparkContext,
					cassandraSparkConnector);
			runUserItemSummary(userItemPropFilePath, sparkContext,
					cassandraSparkConnector);
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ParseException e) {

			e.printStackTrace();
		} catch (Exception e) {

			e.printStackTrace();
		}

	}

	public void runUserItemSummary(String UsrItmSumPropFilePath,
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector) throws Exception {
		PropertiesHandler userItemSummaryProp = new PropertiesHandler(
				UsrItmSumPropFilePath);
		final String USER_ITEM_LEVEL_QUERY_PROPERTY = "UPDATE "
				+ userItemSummaryProp.getValue(PropKeys.OUTPUT_KEYSPACE
						.getValue())
				+ "."
				+ userItemSummaryProp.getValue(PropKeys.OUTPUT_COLUMNFAMILY
						.getValue()) + " SET "
				+ userItemRecoCF.TREND_SCORE.getColumn() + " =?,"
				+ userItemRecoCF.TREND_SCORE_REASON.getColumn() + "=?,"
				+ userItemRecoCF.POPULARITY_SCORE.getColumn() + "=?,"
				+ userItemRecoCF.POPULARITY_SCORE_REASON.getColumn() + "=?,"
				+ userItemRecoCF.DATE.getColumn() + "=?,"
				+ userItemRecoCF.FLAG.getColumn() + "=?";

		CassandraConfig userItemSummaryCassandraProp = new CassandraConfig(
				userItemSummaryProp
						.getValue(PropKeys.INPUT_KEYSPACE.getValue()),
				userItemSummaryProp.getValue(PropKeys.OUTPUT_KEYSPACE
						.getValue()), userItemSummaryProp
						.getValue(PropKeys.INPUT_COLUMNFAMILY.getValue()),
				userItemSummaryProp.getValue(PropKeys.OUTPUT_COLUMNFAMILY
						.getValue()), userItemSummaryProp
						.getValue(PropKeys.PAGE_ROW_SIZE.getValue()),
				USER_ITEM_LEVEL_QUERY_PROPERTY);
		String scoreSummaryCF = userItemSummaryProp
				.getValue(PropKeys.SCORE_SUMMARY.getValue());
		UserItemSummaryService userItemSummaryService = new UserItemSummaryService(
				userItemSummaryProp.getValue(PropKeys.ITEM_LEVEL_CF_KEYSPACE
						.getValue()), scoreSummaryCF,
				userItemSummaryProp
						.getValue(PropKeys.ITEM_LEVEL_CF_PAGE_ROW_SIZE
								.getValue()),
				UserItemRecommendationUtil.getDate(userItemSummaryProp
						.getValue(PropKeys.INPUT_DATE.getValue()), DATE_FORMAT));
		runUserItemSummaryCalculator(sparkContext, cassandraSparkConnector,
				userItemSummaryCassandraProp, userItemSummaryService,
				userItemSummaryProp.getValue(PropKeys.ACCOUNT.getValue()));

	}

	public void runUserItemSummary(String appPropFilePath,
			String userItemLevelSummaryPropFilePath) throws Exception {
		PropertiesHandler appProp = new PropertiesHandler(appPropFilePath);
		JavaSparkContext sparkContext = initSparkContext(appProp);
		CassandraSparkConnector cassandraSparkConnector = initCassandraSparkConn(appProp);
		runUserItemSummary(userItemLevelSummaryPropFilePath, sparkContext,
				cassandraSparkConnector);

	}

	public void runItemSummary(String itemSummaryPropFilePath,
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector) throws Exception {
		PropertiesHandler itemSummaryProp = new PropertiesHandler(
				itemSummaryPropFilePath);
		final String ITEM_LEVEL_SUMMARY_QUERY_PROPERTY = "UPDATE "
				+ itemSummaryProp.getValue(PropKeys.OUTPUT_KEYSPACE.getValue())
				+ "."
				+ itemSummaryProp.getValue(PropKeys.OUTPUT_COLUMNFAMILY
						.getValue()) + " SET "
				+ userItemRecoCF.TREND_SCORE.getColumn() + " =?,"
				+ userItemRecoCF.TREND_SCORE_REASON.getColumn() + "=?,"
				+ userItemRecoCF.POPULARITY_SCORE.getColumn() + "=?,"
				+ userItemRecoCF.POPULARITY_SCORE_REASON.getColumn() + "=?,"
				+ userItemRecoCF.DATE.getColumn() + "=?,"
				+ userItemRecoCF.FLAG.getColumn() + "=?";

		CassandraConfig scoreSummaryCassandraProp = new CassandraConfig(
				itemSummaryProp.getValue(PropKeys.INPUT_KEYSPACE.getValue()),
				itemSummaryProp.getValue(PropKeys.OUTPUT_KEYSPACE.getValue()),
				itemSummaryProp.getValue(PropKeys.INPUT_COLUMNFAMILY.getValue()),
				itemSummaryProp.getValue(PropKeys.OUTPUT_COLUMNFAMILY
						.getValue()), itemSummaryProp
						.getValue(PropKeys.PAGE_ROW_SIZE.getValue()),
				ITEM_LEVEL_SUMMARY_QUERY_PROPERTY);

		Map<String, String> itemLevelRecommendationCF = getItemLevelColumnFamilies(itemSummaryProp);
		ItemSummaryService scoreSummaryService = new ItemSummaryService(
				itemSummaryProp.getValue(PropKeys.ITEM_LEVEL_CF_KEYSPACE
						.getValue()), itemLevelRecommendationCF,
				itemSummaryProp.getValue(PropKeys.ITEM_LEVEL_CF_PAGE_ROW_SIZE
						.getValue()),
				UserItemRecommendationUtil.getDate(itemSummaryProp
						.getValue(PropKeys.INPUT_DATE.getValue()), DATE_FORMAT));
		runScoreSummaryCalculator(sparkContext, cassandraSparkConnector,
				scoreSummaryCassandraProp, scoreSummaryService);

	}

	private void runUserItemSummaryCalculator(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			CassandraConfig userItemSummaryCassandraProp,
			UserItemSummaryService userItemSummaryService,
			String accountColumnFamily) throws Exception {
		Configuration profileConf = new Configuration();
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraProfileRDD = cassandraSparkConnector
				.read(profileConf, sparkContext,
						userItemSummaryCassandraProp.getInputKeyspace(),
						userItemSummaryCassandraProp.getInputColumnfamily(),
						userItemSummaryCassandraProp.getPageRowSize());
		JavaPairRDD<String, String> profileRDD = ProfileTransformation
				.getProfile(cassandraProfileRDD);
		JavaPairRDD<String, String> filteredProfileRDD = Filter
				.filterStringPair(profileRDD);
		Configuration accountConf = new Configuration();
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraAccountRDD = cassandraSparkConnector
				.read(accountConf, sparkContext,
						userItemSummaryCassandraProp.getInputKeyspace(),
						accountColumnFamily,
						userItemSummaryCassandraProp.getPageRowSize());
		JavaPairRDD<String, String> accountRDD = AccountTransformation
				.getAccount(cassandraAccountRDD);

		JavaPairRDD<String, String> filteredAccountRDD = Filter
				.filterStringPair(accountRDD);

		JavaPairRDD<String, String> profileWithTenantRDD = UserItemRecommendationUtil
				.mergeTenant(filteredProfileRDD, filteredAccountRDD);

		JavaRDD<UserItemSummary> userItemScoreRDD = userItemSummaryService
				.calculateUserItemSummary(sparkContext,
						cassandraSparkConnector, profileWithTenantRDD);

		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraUserItemSummaryRDD = UserItemRecoPostprocessing
				.processingUserItemScoreSummary(userItemScoreRDD);
		Configuration outputConf = new Configuration();
		cassandraSparkConnector.write(outputConf,
				userItemSummaryCassandraProp.getOutputKeyspace(),
				userItemSummaryCassandraProp.getOutputColumnfamily(),
				userItemSummaryCassandraProp.getOutputQuery(),
				cassandraUserItemSummaryRDD);
	}

	private void runScoreSummaryCalculator(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			CassandraConfig scoreSummaryCassandraProp,
			ItemSummaryService scoreSummaryService) throws Exception {
		Configuration conf = new Configuration();
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraItemRDD = cassandraSparkConnector
				.read(conf, sparkContext,
						scoreSummaryCassandraProp.getInputKeyspace(),
						scoreSummaryCassandraProp.getInputColumnfamily(),
						scoreSummaryCassandraProp.getPageRowSize());
		JavaPairRDD<String, String> itemRDD = ItemTransformation
				.getItem(cassandraItemRDD);

		JavaPairRDD<String, String> filteredItemRDD = Filter
				.filterStringPair(itemRDD);
		JavaRDD<ItemSummary> scoreSummaryRDD = scoreSummaryService
				.calculateScoreSummary(sparkContext, cassandraSparkConnector,
						filteredItemRDD);
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraScoreSummaryRDD = UserItemRecoPostprocessing
				.processingScoreSummary(scoreSummaryRDD);
		cassandraSparkConnector.write(conf,
				scoreSummaryCassandraProp.getOutputKeyspace(),
				scoreSummaryCassandraProp.getOutputColumnfamily(),
				scoreSummaryCassandraProp.getOutputQuery(),
				cassandraScoreSummaryRDD);
	}

	private Map<String, String> getItemLevelColumnFamilies(
			PropertiesHandler scoreSummaryProp) throws IOException {
		Map<String, String> itemLevelColumnFamilies = new HashMap<String, String>();
		itemLevelColumnFamilies.put(PropKeys.TREND.getValue(),
				scoreSummaryProp.getValue(PropKeys.TREND.getValue()));
		itemLevelColumnFamilies.put(PropKeys.POPULARITY.getValue(),
				scoreSummaryProp.getValue(PropKeys.POPULARITY.getValue()));
		return itemLevelColumnFamilies;
	}

	public void runItemLevelScoreSummary(String appPropFilePath,
			String scoreSummaryPropFilePath) throws Exception {
		PropertiesHandler appProp = new PropertiesHandler(appPropFilePath);

		JavaSparkContext sparkContext = initSparkContext(appProp);
		CassandraSparkConnector cassandraSparkConnector = initCassandraSparkConn(appProp);
		runItemSummary(scoreSummaryPropFilePath, sparkContext,
				cassandraSparkConnector);

	}

	public CassandraSparkConnector initCassandraSparkConn(
			PropertiesHandler appProp) throws IOException {

		CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
				UserItemRecommendationUtil.getList(
						appProp.getValue(PropKeys.INPUT_HOST_LIST.getValue()),
						","), appProp.getValue(PropKeys.INPUT_PARTITIONER
						.getValue()), appProp.getValue(PropKeys.INPUT_RPC_PORT
						.getValue()), UserItemRecommendationUtil.getList(
						appProp.getValue(PropKeys.OUTPUT_HOST_LIST.getValue()),
						","), appProp.getValue(PropKeys.OUTPUT_PARTITIONER
						.getValue()));
		return cassandraSparkConnector;
	}

	public JavaSparkContext initSparkContext(PropertiesHandler appProp)
			throws IOException {
		JavaSparkContext sparkContext = new JavaSparkContext(
				appProp.getValue(PropKeys.MODE_PROPERTY.getValue()),
				appProp.getValue(PropKeys.APP_NAME.getValue()));
		return sparkContext;
	}

	public static void main(String[] args) {
		UserItemRecoDriver userItemRecoDriver = new UserItemRecoDriver();
		if(args.length==3){
		userItemRecoDriver
				.runUserItemRecoDriver(
						args[0],
						args[1],
						args[2]);
		}
		else{
			System.out.println("Please provide the property file paths\n 1. Application property file path \n 2. Property file path of item summary \n 3. Property file path of user item summary");
		}
	}

}
