package com.dla.foundation.useritemreco;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.useritemreco.model.CassandraConfig;
import com.dla.foundation.useritemreco.model.ItemSummary;
import com.dla.foundation.useritemreco.model.UserItemSummary;
import com.dla.foundation.useritemreco.model.userItemRecoCF;
import com.dla.foundation.useritemreco.util.AccountTransformation;
import com.dla.foundation.useritemreco.util.Filter;
import com.dla.foundation.useritemreco.util.ItemTransformation;
import com.dla.foundation.useritemreco.util.ProfileTransformation;
import com.dla.foundation.useritemreco.util.PropKeys;
import com.dla.foundation.useritemreco.util.UserItemRecoPostprocess;
import com.dla.foundation.useritemreco.util.UserItemRecommendationUtil;

/**
 * This class is driver class whose functionality is to initialize property for
 * item summary and user item summary and invoke them. This class also provide
 * independent existence of item summary and user item summary through
 * overloaded methods.
 * 
 * @author shishir_shivhare
 * 
 */
public class UserItemRecoDriver implements Serializable {


	private static final long serialVersionUID = -4424270924188805825L;
	private static final String DATE_FORMAT = "yyyy-MM-dd";
	private static final Logger logger = Logger
			.getLogger(UserItemRecoDriver.class);

	public void runUserItemRecoDriver(String appPropFilePath,
			String itmSumPropFilePath, String userItemPropFilePath)
					throws Exception {
		PropertiesHandler appProp;

		logger.info("initializing application level property ");
		appProp = new PropertiesHandler(appPropFilePath);
		logger.info("initializing spark context ");
		JavaSparkContext sparkContext = initSparkContext(appProp);
		logger.info("initializing cassandra spark connector ");
		CassandraSparkConnector cassandraSparkConnector = initCassandraSparkConn(appProp);
		logger.info("invoking item summary");
		runItemSummary(itmSumPropFilePath, sparkContext,
				cassandraSparkConnector);
		logger.info("invoking user item summary");
		runUserItemSummary(userItemPropFilePath, sparkContext,
				cassandraSparkConnector);

	}

	/**
	 * This method will be used to initialize all the property and providing to
	 * user item summary calculator.
	 * 
	 * @param UsrItmSumPropFilePath
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @throws Exception
	 */
	public void runUserItemSummary(String UsrItmSumPropFilePath,
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector) throws Exception {
		PropertiesHandler userItemSummaryProp = new PropertiesHandler(
				UsrItmSumPropFilePath);
		logger.info("initializing query for user item level summary");
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
								+ userItemRecoCF.FP_SCORE.getColumn() + " =?,"
								+ userItemRecoCF.FP_SCORE_REASON.getColumn() + "=?,"
								+ userItemRecoCF.NEW_RELEASE_SCORE.getColumn() + " =?,"
								+ userItemRecoCF.NEW_RELEASE_SCORE_REASON.getColumn() + "=?,"
								+ userItemRecoCF.SOCIAL_SCORE.getColumn() + "=?,"
								+ userItemRecoCF.SOCIAL_SCORE_REASON.getColumn() + "=?,"
								+ userItemRecoCF.PIO_SCORE.getColumn() + "=?,"
								+ userItemRecoCF.PIO_SCORE_REASON.getColumn() + "=?,"
								+ userItemRecoCF.DATE.getColumn() + "=?,"
								+ userItemRecoCF.EVENT_REQUIRED.getColumn() + "=?";

		logger.info("initializing cassandra configuration");
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
		Map<String, String> userItemReco = getUserItemRecoInfo(userItemSummaryProp);
		UserItemSummaryCalc userItemSummaryService = new UserItemSummaryCalc(
				userItemSummaryProp.getValue(PropKeys.ITEM_LEVEL_CF_KEYSPACE
						.getValue()),
						scoreSummaryCF,
						userItemSummaryProp
						.getValue(PropKeys.ITEM_LEVEL_CF_PAGE_ROW_SIZE
								.getValue()),
								UserItemRecommendationUtil.getDate(userItemSummaryProp
										.getValue(PropKeys.INPUT_DATE.getValue()), DATE_FORMAT),
										userItemReco);
		Map<String, String> userItemPreferredRegionInfo = getUserPreferredRegion(userItemSummaryProp);
		logger.info("invoking user item summary calculator");
		runUserItemSummaryCalculator(sparkContext, cassandraSparkConnector,
				userItemSummaryCassandraProp, userItemSummaryService,
				userItemSummaryProp.getValue(PropKeys.ACCOUNT.getValue()),
				userItemPreferredRegionInfo);

	}

	

	public void runUserItemSummary(String appPropFilePath,
			String userItemLevelSummaryPropFilePath) throws Exception {
		PropertiesHandler appProp = new PropertiesHandler(appPropFilePath);
		JavaSparkContext sparkContext = initSparkContext(appProp);
		CassandraSparkConnector cassandraSparkConnector = initCassandraSparkConn(appProp);
		runUserItemSummary(userItemLevelSummaryPropFilePath, sparkContext,
				cassandraSparkConnector);

	}

	/**
	 * This method will be used to initialize all the property and providing to
	 * item summary calculator.
	 * 
	 * @param itemSummaryPropFilePath
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @throws Exception
	 */
	public void runItemSummary(String itemSummaryPropFilePath,
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector) throws Exception {
		PropertiesHandler itemSummaryProp = new PropertiesHandler(
				itemSummaryPropFilePath);
		logger.info("initializing query for item level summary");
		final String ITEM_LEVEL_SUMMARY_QUERY_PROPERTY = "UPDATE "
				+ itemSummaryProp.getValue(PropKeys.OUTPUT_KEYSPACE.getValue())
				+ "."
				+ itemSummaryProp.getValue(PropKeys.OUTPUT_COLUMNFAMILY
						.getValue()) + " SET "
						+ userItemRecoCF.TREND_SCORE.getColumn() + " =?,"
						+ userItemRecoCF.TREND_SCORE_REASON.getColumn() + "=?,"
						+ userItemRecoCF.POPULARITY_SCORE.getColumn() + "=?,"
						+ userItemRecoCF.POPULARITY_SCORE_REASON.getColumn() + "=?,"
						+ userItemRecoCF.FP_SCORE.getColumn() + "=?,"
						+ userItemRecoCF.FP_SCORE_REASON.getColumn() + "=?,"
						+ userItemRecoCF.NEW_RELEASE_SCORE.getColumn() + "=?,"
						+ userItemRecoCF.NEW_RELEASE_SCORE_REASON.getColumn() + "=?,"
						+ userItemRecoCF.DATE.getColumn() + "=?,"
						+ userItemRecoCF.EVENT_REQUIRED.getColumn() + "=?";

		logger.info("initializing cassandra configuration");
		CassandraConfig scoreSummaryCassandraProp = new CassandraConfig(
				itemSummaryProp.getValue(PropKeys.INPUT_KEYSPACE.getValue()),
				itemSummaryProp.getValue(PropKeys.OUTPUT_KEYSPACE.getValue()),
				itemSummaryProp.getValue(PropKeys.INPUT_COLUMNFAMILY.getValue()),
				itemSummaryProp.getValue(PropKeys.OUTPUT_COLUMNFAMILY
						.getValue()), itemSummaryProp
						.getValue(PropKeys.PAGE_ROW_SIZE.getValue()),
						ITEM_LEVEL_SUMMARY_QUERY_PROPERTY);

		logger.info("fetching all item level column families");
		Map<String, String> itemLevelRecommendationCF = getItemLevelColumnFamilies(itemSummaryProp);

		logger.info("initializing item summary Calc");
		ItemSummaryCalc ItemSummaryCalc = new ItemSummaryCalc(
				itemSummaryProp.getValue(PropKeys.ITEM_LEVEL_CF_KEYSPACE
						.getValue()), itemLevelRecommendationCF,
						itemSummaryProp.getValue(PropKeys.ITEM_LEVEL_CF_PAGE_ROW_SIZE
								.getValue()),
								UserItemRecommendationUtil.getDate(itemSummaryProp
										.getValue(PropKeys.INPUT_DATE.getValue()), DATE_FORMAT));

		logger.info("invoking item summary calculator");
		Map<String, String> itemRegionTenantInfo = getRegionTenantInfo(itemSummaryProp);
		runItemSummaryCalculator(sparkContext, cassandraSparkConnector,
				scoreSummaryCassandraProp, ItemSummaryCalc,
				itemRegionTenantInfo);

	}

	/**
	 * This function will fetch profile and account column family, perform inner
	 * join between then to get tenant and then call user item summary to
	 * convert item level summary to user item level summary
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param userItemSummaryCassandraProp
	 * @param userItemSummaryCalc
	 * @param accountColumnFamily
	 * @throws Exception
	 */
	private void runUserItemSummaryCalculator(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			CassandraConfig userItemSummaryCassandraProp,
			UserItemSummaryCalc userItemSummaryCalc,
			String accountColumnFamily, Map<String, String> userItemPreferredRegionInfo)
					throws Exception {
		Configuration profileConf = new Configuration();

		logger.info("reading from profile column family");
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraProfileRDD = cassandraSparkConnector
				.read(profileConf, sparkContext,
						userItemSummaryCassandraProp.getInputKeyspace(),
						userItemSummaryCassandraProp.getInputColumnfamily(),
						userItemSummaryCassandraProp.getPageRowSize());



		logger.info("transforming profile column family");
		JavaPairRDD<String, String> profileRDD = ProfileTransformation
				.getProfile(cassandraProfileRDD, userItemPreferredRegionInfo);

		logger.info("filtering profile column family");
		JavaPairRDD<String, String> filteredProfileRDD = Filter
				.filterStringPair(profileRDD);

		logger.info("reading from account column family");
		Configuration accountConf = new Configuration();
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraAccountRDD = cassandraSparkConnector
				.read(accountConf, sparkContext,
						userItemSummaryCassandraProp.getInputKeyspace(),
						accountColumnFamily,
						userItemSummaryCassandraProp.getPageRowSize());

		logger.info("transforming account column family");
		JavaPairRDD<String, String> accountRDD = AccountTransformation
				.getAccount(cassandraAccountRDD);

		logger.info("filtering account column family");
		JavaPairRDD<String, String> filteredAccountRDD = Filter
				.filterStringPair(accountRDD);

		logger.info("joining account & profile column family");
		JavaPairRDD<String, String> profileWithTenantRDD = UserItemRecommendationUtil
				.mergeTenant(filteredProfileRDD, filteredAccountRDD);

		logger.info("invoking user item summary Calc");
		JavaRDD<UserItemSummary> userItemScoreRDD = userItemSummaryCalc
				.calculateUserItemSummary(sparkContext,
						cassandraSparkConnector, profileWithTenantRDD);

		logger.info("transforming the result of user item reco into cassandra format before writing to cassandra");
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraUserItemSummaryRDD = UserItemRecoPostprocess
				.processUserItemScoreSummary(userItemScoreRDD);

		logger.info("Writing to cassandra column family");
		Configuration outputConf = new Configuration();
		cassandraSparkConnector.write(outputConf,
				userItemSummaryCassandraProp.getOutputKeyspace(),
				userItemSummaryCassandraProp.getOutputColumnfamily(),
				userItemSummaryCassandraProp.getOutputQuery(),
				cassandraUserItemSummaryRDD);
	}

	/**
	 * This function will provide the functionality of reading from item column
	 * family ,invoking item calc with required parameter and writing result to
	 * specified column family.
	 * 
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @param scoreSummaryCassandraProp
	 * @param itemSummaryCalc
	 * @throws Exception
	 */
	private void runItemSummaryCalculator(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			CassandraConfig scoreSummaryCassandraProp,
			ItemSummaryCalc itemSummaryCalc, Map<String, String> itemRegionTenantInfo)
					throws Exception {
		Configuration conf = new Configuration();

		logger.info("reading item column family");
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraItemRDD = cassandraSparkConnector
				.read(conf, sparkContext,
						scoreSummaryCassandraProp.getInputKeyspace(),
						scoreSummaryCassandraProp.getInputColumnfamily(),
						scoreSummaryCassandraProp.getPageRowSize());


		logger.info("transforming item column family");
		JavaPairRDD<String, String> itemRDD = ItemTransformation.getItem(
				cassandraItemRDD, itemRegionTenantInfo);

		logger.info("filtering record of item column family");
		JavaPairRDD<String, String> filteredItemRDD = Filter
				.filterStringPair(itemRDD);
		logger.info("invoking item summary Calc");
		JavaRDD<ItemSummary> scoreSummaryRDD = itemSummaryCalc
				.calculateScoreSummary(sparkContext, cassandraSparkConnector,
						filteredItemRDD);
		logger.info("transforming record to convert to cassandra format to write in intermediate column family");
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraScoreSummaryRDD = UserItemRecoPostprocess
				.processScoreSummary(scoreSummaryRDD);
		logger.info("writing to intermediate column family");
		cassandraSparkConnector.write(conf,
				scoreSummaryCassandraProp.getOutputKeyspace(),
				scoreSummaryCassandraProp.getOutputColumnfamily(),
				scoreSummaryCassandraProp.getOutputQuery(),
				cassandraScoreSummaryRDD);
	}

	/**
	 * This functio fetch item level column family from property file
	 * 
	 * @param scoreSummaryProp
	 * @return
	 * @throws IOException
	 */
	private Map<String, String> getItemLevelColumnFamilies(
			PropertiesHandler scoreSummaryProp) throws IOException {
		Map<String, String> itemLevelColumnFamilies = new HashMap<String, String>();
		itemLevelColumnFamilies.put(PropKeys.TREND.getValue(),
				scoreSummaryProp.getValue(PropKeys.TREND.getValue()));
		itemLevelColumnFamilies.put(PropKeys.POPULARITY.getValue(),
				scoreSummaryProp.getValue(PropKeys.POPULARITY.getValue()));
		itemLevelColumnFamilies.put(PropKeys.FP.getValue(),
				scoreSummaryProp.getValue(PropKeys.FP.getValue()));
		itemLevelColumnFamilies.put(PropKeys.NEW_RELEASE.getValue(),
				scoreSummaryProp.getValue(PropKeys.NEW_RELEASE.getValue()));
		return itemLevelColumnFamilies;
	}
	
	private Map<String,String> getUserItemRecoInfo(PropertiesHandler userItemSummaryProp) throws IOException
	{
		Map<String, String> userItemReco = new HashMap<String, String>();
		userItemReco.put(PropKeys.SOCIAL_RECOMMENDATION.getValue(),
				userItemSummaryProp.getValue(PropKeys.SOCIAL_RECOMMENDATION
						.getValue()));
		userItemReco.put(PropKeys.PIO_RECOMMENDATION.getValue(),
				userItemSummaryProp.getValue(PropKeys.PIO_RECOMMENDATION
						.getValue()));
		return userItemReco;
	}

	private Map<String,String> getRegionTenantInfo(PropertiesHandler itemSummaryProp) throws IOException
	{
		Map<String, String> itemRegionTenantInfo = new HashMap<String, String>();
		itemRegionTenantInfo.put(PropKeys.REGION_ID.getValue(),
				itemSummaryProp.getValue(PropKeys.REGION_ID.getValue()));
		itemRegionTenantInfo.put(PropKeys.TENANT_ID.getValue(),
				itemSummaryProp.getValue(PropKeys.TENANT_ID.getValue()));
		return itemRegionTenantInfo;
	}

	private Map<String,String> getUserPreferredRegion(PropertiesHandler userItemSummaryProp) throws IOException
	{
		Map<String, String> userItemPreferredRegionInfo = new HashMap<String, String>();
		userItemPreferredRegionInfo.put(PropKeys.PREFFRED_REGION_ID.getValue(),
				userItemSummaryProp.getValue(PropKeys.PREFFRED_REGION_ID
						.getValue()));
		return userItemPreferredRegionInfo;
	}

	/**
	 * This is a overloaded function which provide the functionality of
	 * independently invoking the item summary.
	 * 
	 * @param appPropFilePath
	 * @param scoreSummaryPropFilePath
	 * @throws Exception
	 */
	public void runItemSummary(String appPropFilePath,
			String scoreSummaryPropFilePath) throws Exception {
		PropertiesHandler appProp = new PropertiesHandler(appPropFilePath);

		JavaSparkContext sparkContext = initSparkContext(appProp);
		CassandraSparkConnector cassandraSparkConnector = initCassandraSparkConn(appProp);
		runItemSummary(scoreSummaryPropFilePath, sparkContext,
				cassandraSparkConnector);

	}

	/**
	 * This function initializes the cassandra spark connector
	 * 
	 * @param appProp
	 * @return
	 * @throws IOException
	 */
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

	/**
	 * This function initializes the spark context
	 * 
	 * @param appProp
	 * @return
	 * @throws IOException
	 */
	public JavaSparkContext initSparkContext(PropertiesHandler appProp)
			throws IOException {
		JavaSparkContext sparkContext = new JavaSparkContext(
				appProp.getValue(PropKeys.MODE_PROPERTY.getValue()),
				appProp.getValue(PropKeys.APP_NAME.getValue()));
		return sparkContext;
	}

	public static void main(String[] args) throws Exception {
		UserItemRecoDriver userItemRecoDriver = new UserItemRecoDriver();
		if (args.length == 3) {
			userItemRecoDriver.runUserItemRecoDriver(args[0], args[1], args[2]);
		} else {
			System.out
			.println("Please provide the property file paths\n 1. Application property file path \n 2. Property file path of item summary \n 3. Property file path of user item summary");
		}
	}

}