package com.dla.foundation.useritemreco;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.useritemreco.model.CassandraConfig;
import com.dla.foundation.useritemreco.model.ItemSummary;
import com.dla.foundation.useritemreco.model.UserItemSummary;
import com.dla.foundation.useritemreco.model.UserItemRecoCF;
import com.dla.foundation.useritemreco.util.AccountTransformation;
import com.dla.foundation.useritemreco.util.Filter;
import com.dla.foundation.useritemreco.util.ItemTransformation;
import com.dla.foundation.useritemreco.util.ProfileTransformation;
import com.dla.foundation.useritemreco.util.PropKeys;
import com.dla.foundation.useritemreco.util.UserItemRecoPostprocess;
import com.dla.foundation.useritemreco.util.UserItemRecoProp;
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

	public void runUserItemRecoDriver(String commonPropFilePath)
			throws IOException, ParseException {
		PropertiesHandler userItemRecoProp = null;
		try {
			userItemRecoProp = new PropertiesHandler(commonPropFilePath,
					UserItemRecoProp.USER_ITEM_RECO_APP_NAME);
			logger.info("initializing application level property ");
			logger.info("initializing spark context ");
			JavaSparkContext sparkContext = initSparkContext(userItemRecoProp);
			logger.info("initializing cassandra spark connector ");
			CassandraSparkConnector cassandraSparkConnector = initCassandraSparkConn(userItemRecoProp);
			logger.info("invoking item summary");
			runItemSummary(userItemRecoProp, sparkContext,
					cassandraSparkConnector);
			logger.info("invoking user item summary");
			runUserItemSummary(userItemRecoProp, sparkContext,
					cassandraSparkConnector);
			Date outputDate = DateUtils.addDays(UserItemRecommendationUtil
					.getDate(userItemRecoProp.getValue(PropKeys.INPUT_DATE
							.getValue()), DATE_FORMAT), 1);
			userItemRecoProp
					.writeToCassandra(PropKeys.INPUT_DATE.getValue(),
							UserItemRecommendationUtil.getDate(outputDate,
									DATE_FORMAT));
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new IOException(e.getMessage());

		} finally {
			userItemRecoProp.close();
		}
	}

	/**
	 * This method will be used to initialize all the property and providing to
	 * user item summary calculator.
	 * 
	 * @param userItemRecoProp
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @throws IOException
	 * @throws ParseException
	 */
	public void runUserItemSummary(PropertiesHandler userItemRecoProp,
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector)
			throws IOException, ParseException {

		logger.info("initializing query for user item level summary");
		final String USER_ITEM_LEVEL_QUERY_PROPERTY = "UPDATE "
				+ userItemRecoProp.getValue(CommonPropKeys.cs_fisKeyspace
						.getValue()) + "." + UserItemRecoProp.OUTPUT_CF
				+ " SET " + UserItemRecoCF.TREND_SCORE.getColumn() + " =?,"
				+ UserItemRecoCF.TREND_SCORE_REASON.getColumn() + "=?,"
				+ UserItemRecoCF.POPULARITY_SCORE.getColumn() + "=?,"
				+ UserItemRecoCF.POPULARITY_SCORE_REASON.getColumn() + "=?,"
				+ UserItemRecoCF.NEW_RELEASE_SCORE.getColumn() + " =?,"
				+ UserItemRecoCF.NEW_RELEASE_SCORE_REASON.getColumn() + "=?,"
				+ UserItemRecoCF.SOCIAL_SCORE.getColumn() + "=?,"
				+ UserItemRecoCF.SOCIAL_SCORE_REASON.getColumn() + "=?,"
				+ UserItemRecoCF.PIO_SCORE.getColumn() + "=?,"
				+ UserItemRecoCF.PIO_SCORE_REASON.getColumn() + "=?,"
				+ UserItemRecoCF.DATE.getColumn() + "=?,"
				+ UserItemRecoCF.EVENT_REQUIRED.getColumn() + "=?,"
				+ UserItemRecoCF.JUSTIFICATION.getColumn() + "=?";

		logger.info("initializing cassandra configuration");
		CassandraConfig userItemSummaryCassandraProp = new CassandraConfig(
				userItemRecoProp.getValue(CommonPropKeys.cs_platformKeyspace
						.getValue()),
				userItemRecoProp.getValue(CommonPropKeys.cs_fisKeyspace
						.getValue()), UserItemRecoProp.INPUT_CF_PROFILE,
				UserItemRecoProp.OUTPUT_CF,
				userItemRecoProp.getValue(CommonPropKeys.cs_pageRowSize),
				USER_ITEM_LEVEL_QUERY_PROPERTY);

		String scoreSummaryCF = UserItemRecoProp.ITEM_LEVEL_SCORE_SUMMARY_CF;

		UserItemSummaryCalc userItemSummaryService = new UserItemSummaryCalc(
				userItemRecoProp.getValue(CommonPropKeys.cs_fisKeyspace
						.getValue()), scoreSummaryCF,
				userItemRecoProp.getValue(CommonPropKeys.cs_fisKeyspace
						.getValue()), UserItemRecommendationUtil.getDate(
						userItemRecoProp.getValue(PropKeys.INPUT_DATE
								.getValue()), DATE_FORMAT));

		logger.info("invoking user item summary calculator");
		runUserItemSummaryCalculator(sparkContext, cassandraSparkConnector,
				userItemSummaryCassandraProp, userItemSummaryService,
				UserItemRecoProp.ACCOUNT_CF);

	}

	public void runUserItemSummary(String commonPropFilePath)
			throws IOException, ParseException {
		PropertiesHandler userItemRecoProp = new PropertiesHandler(
				commonPropFilePath, UserItemRecoProp.USER_ITEM_RECO_APP_NAME);
		JavaSparkContext sparkContext = initSparkContext(userItemRecoProp);
		CassandraSparkConnector cassandraSparkConnector = initCassandraSparkConn(userItemRecoProp);
		runUserItemSummary(userItemRecoProp, sparkContext,
				cassandraSparkConnector);
	}

	/**
	 * This method will be used to initialize all the property and providing to
	 * item summary calculator.
	 * 
	 * @param itemSummaryPropFilePath
	 * @param sparkContext
	 * @param cassandraSparkConnector
	 * @throws IOException
	 * @throws ParseException
	 */
	public void runItemSummary(PropertiesHandler userItemRecoProp,
			JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector)
			throws IOException, ParseException {
		logger.info("initializing query for item level summary");
		final String ITEM_LEVEL_SUMMARY_QUERY_PROPERTY = "UPDATE "
				+ userItemRecoProp.getValue(CommonPropKeys.cs_fisKeyspace
						.getValue()) + "." + UserItemRecoProp.OUTPUT_CF_ITEM
				+ " SET " + UserItemRecoCF.TREND_SCORE.getColumn() + " =?,"
				+ UserItemRecoCF.TREND_SCORE_REASON.getColumn() + "=?,"
				+ UserItemRecoCF.POPULARITY_SCORE.getColumn() + "=?,"
				+ UserItemRecoCF.POPULARITY_SCORE_REASON.getColumn() + "=?,"
				+ UserItemRecoCF.NEW_RELEASE_SCORE.getColumn() + "=?,"
				+ UserItemRecoCF.NEW_RELEASE_SCORE_REASON.getColumn() + "=?,"
				+ UserItemRecoCF.DATE.getColumn() + "=?,"
				+ UserItemRecoCF.EVENT_REQUIRED.getColumn() + "=?";

		logger.info("initializing cassandra configuration");
		CassandraConfig scoreSummaryCassandraProp = new CassandraConfig(
				userItemRecoProp.getValue(CommonPropKeys.cs_platformKeyspace
						.getValue()),
				userItemRecoProp.getValue(CommonPropKeys.cs_fisKeyspace
						.getValue()), UserItemRecoProp.INPUT_CF_ITEM,
				UserItemRecoProp.OUTPUT_CF_ITEM,
				userItemRecoProp.getValue(CommonPropKeys.cs_pageRowSize
						.getValue()), ITEM_LEVEL_SUMMARY_QUERY_PROPERTY);

		logger.info("initializing item summary Calc");
		ItemSummaryCalc ItemSummaryCalc = new ItemSummaryCalc(
				userItemRecoProp.getValue(CommonPropKeys.cs_fisKeyspace
						.getValue()),
				userItemRecoProp.getValue(CommonPropKeys.cs_pageRowSize
						.getValue()), UserItemRecommendationUtil.getDate(
						userItemRecoProp.getValue(PropKeys.INPUT_DATE
								.getValue()), DATE_FORMAT));

		logger.info("invoking item summary calculator");
		runItemSummaryCalculator(sparkContext, cassandraSparkConnector,
				scoreSummaryCassandraProp, ItemSummaryCalc);

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
	 */
	private void runUserItemSummaryCalculator(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			CassandraConfig userItemSummaryCassandraProp,
			UserItemSummaryCalc userItemSummaryCalc, String accountColumnFamily) {
		Configuration profileConf = new Configuration();

		logger.info("reading from profile column family");
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraProfileRDD = cassandraSparkConnector
				.read(profileConf, sparkContext,
						userItemSummaryCassandraProp.getInputKeyspace(),
						userItemSummaryCassandraProp.getInputColumnfamily(),
						userItemSummaryCassandraProp.getPageRowSize());

		logger.info("transforming profile column family");
		JavaPairRDD<String, String> profileRDD = ProfileTransformation
				.getProfile(cassandraProfileRDD);

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
	 */
	private void runItemSummaryCalculator(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			CassandraConfig scoreSummaryCassandraProp,
			ItemSummaryCalc itemSummaryCalc) {
		Configuration conf = new Configuration();

		logger.info("reading item column family");
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraItemRDD = cassandraSparkConnector
				.read(conf, sparkContext,
						scoreSummaryCassandraProp.getInputKeyspace(),
						scoreSummaryCassandraProp.getInputColumnfamily(),
						scoreSummaryCassandraProp.getPageRowSize());

		logger.info("transforming item column family");
		JavaPairRDD<String, String> itemRDD = ItemTransformation
				.getItem(cassandraItemRDD);

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
	 * This is a overloaded function which provide the functionality of
	 * independently invoking the item summary.
	 * 
	 * @param appPropFilePath
	 * @param scoreSummaryPropFilePath
	 * @throws IOException
	 * @throws ParseException
	 */
	public void runItemSummary(String commonPropFilePath) throws IOException,
			ParseException {
		PropertiesHandler commonProp = new PropertiesHandler(commonPropFilePath);
		JavaSparkContext sparkContext = initSparkContext(commonProp);
		CassandraSparkConnector cassandraSparkConnector = initCassandraSparkConn(commonProp);
		runItemSummary(commonProp, sparkContext, cassandraSparkConnector);

	}

	/**
	 * This function initializes the cassandra spark connector
	 * 
	 * @param userItemRecoProp
	 * @return
	 * @throws IOException
	 */
	public CassandraSparkConnector initCassandraSparkConn(
			PropertiesHandler userItemRecoProp) throws IOException {

		CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
				UserItemRecommendationUtil.getList(userItemRecoProp
						.getValue(CommonPropKeys.cs_hostList.getValue()), ","),
				UserItemRecoProp.PARTITIONER,
				userItemRecoProp.getValue(CommonPropKeys.cs_rpcPort.getValue()),
				UserItemRecommendationUtil.getList(userItemRecoProp
						.getValue(CommonPropKeys.cs_hostList.getValue()), ","),
				UserItemRecoProp.PARTITIONER);
		return cassandraSparkConnector;
	}

	/**
	 * This function initializes the spark context
	 * 
	 * @param userItemRecoProp
	 * @return
	 * @throws IOException
	 */
	public JavaSparkContext initSparkContext(PropertiesHandler userItemRecoProp)
			throws IOException {
		JavaSparkContext sparkContext = new JavaSparkContext(
				userItemRecoProp.getValue(CommonPropKeys.spark_host.getValue()),
				UserItemRecoProp.USER_ITEM_RECO_APP_NAME);
		return sparkContext;
	}

	public static void main(String[] args) throws IOException, ParseException {
		UserItemRecoDriver userItemRecoDriver = new UserItemRecoDriver();
		if (args.length == 1) {
			userItemRecoDriver.runUserItemRecoDriver(args[0]);
		} else {
			logger.error("Please provide the common properties file paths");
			System.err
					.println("Please provide the common properties file paths");
		}
	}

}