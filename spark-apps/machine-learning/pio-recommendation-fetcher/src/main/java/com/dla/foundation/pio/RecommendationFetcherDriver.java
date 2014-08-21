package com.dla.foundation.pio;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.pio.entity.UserProfile;
import com.dla.foundation.pio.util.CassandraConfig;
import com.dla.foundation.pio.util.ColumnCollection;
import com.dla.foundation.pio.util.PIOConfig;
import com.dla.foundation.pio.util.RecoFetcherConstants;

public class RecommendationFetcherDriver implements Serializable {

	private static final long serialVersionUID = 2894456804103519801L;
	private static Logger logger = Logger
			.getLogger(RecommendationFetcherDriver.class.getName());

	/**
	 * This method fetches recommendations from PIO and saves results to
	 * Cassandra table.
	 * 
	 * @param cassandraSparkConnector
	 *            Instance of CassandraSparkConnector to save results to
	 *            Cassandra table.
	 * @param cassandraConfig
	 *            Instance of CassandraPIOConfig, which holds information for
	 *            Cassandra write, like Cassandra
	 *            keySpace,columnFamily,primary-key etc.
	 * @param pioConfig
	 *            Instance of PIOConfig, which holds holds information for PIO
	 *            interactions, like appName,appURL, engineName etc.
	 * @param sparkAppMaster
	 *            Config parameter Master for instantiating SparkContext.
	 * 
	 */

	public void fetchRecommendations(
			CassandraSparkConnector cassandraSparkConnector,
			CassandraConfig cassandraConfig, PIOConfig pioConfig,
			String sparkAppMaster) {
		
		logger.info("Forming update query for " + cassandraConfig.fisKeySpace
				+ "." + cassandraConfig.recommendationColFamily);
		final String recommendationUpdateQuery = "UPDATE "
				+ cassandraConfig.fisKeySpace + "."
				+ cassandraConfig.recommendationColFamily + " SET "
				+ ColumnCollection.TENANT_ID + "=?,"
				+ ColumnCollection.REGION_ID + "=?,"
				+ ColumnCollection.LAST_MODIFIED + "=?,"
				+ ColumnCollection.RECO_SCORE + "=?,"
				+ ColumnCollection.RECO_REASON + "=?,"
				+ ColumnCollection.EVENT_REQUIRED + "=?";

		SparkPIOConnector sparkPIOConnector = new SparkPIOConnector();
		// Instantiates JavaSparkContext
		logger.info("Initializing java spark context");
		JavaSparkContext sparkContext = sparkPIOConnector
				.initilizeSparkContext(sparkAppMaster,
						RecoFetcherConstants.APPNAME);
		logger.info("Initialized java spark context successfully");

		logger.info("Reading records from " + cassandraConfig.profileColFamily);
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> profileCassandraRDD = cassandraSparkConnector
				.read(new Configuration(), sparkContext,
						cassandraConfig.platformKeySpace,
						cassandraConfig.profileColFamily,
						cassandraConfig.pageRowSize, new String[] {
								ColumnCollection.ID,
								ColumnCollection.ACCOUNT_ID,
								ColumnCollection.HOME_REGION_ID });
		logger.info("Reading records from " + cassandraConfig.accountColFamily);
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> accountCassandraRDD = cassandraSparkConnector
				.read(new Configuration(), sparkContext,
						cassandraConfig.platformKeySpace,
						cassandraConfig.accountColFamily,
						cassandraConfig.pageRowSize,
						new String[] { ColumnCollection.ID,
								ColumnCollection.TENANT_ID });

		logger.info("Converting profile data read from Cassandra to profilePairRDD");
		JavaPairRDD<String, String> profilePairRDD = sparkPIOConnector
				.toProfilePairRDD(profileCassandraRDD);
		logger.info("Converting account data read from Cassandra to accountPairRDD");
		JavaPairRDD<String, String> accountPairRDDAllTenant = sparkPIOConnector
				.toAccountPairRDD(accountCassandraRDD);

		JavaRDD<UserProfile> userProfileRDD = getUserProfile(profilePairRDD,
				accountPairRDDAllTenant);

		// Get recommendation for Users read from Cassandra using PIO
		JavaPairRDD<UserProfile, List<String>> userRecommendationsRDD = getRecommendations(
				userProfileRDD, pioConfig);
		JavaPairRDD<UserProfile, String> userPerRecoRDD = sparkPIOConnector
				.toUserProfilePerRecoRDD(userRecommendationsRDD);
		// Converts Primary Keys to Map<String, ByteBuffer> and Other values to
		// List<ByteBuffer>
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraRDD = sparkPIOConnector
				.formatRecommendationsForCassandraWrite(userPerRecoRDD);
		logger.info("Writting user recommendations to Cassandra");
		cassandraSparkConnector.write(new Configuration(),
				cassandraConfig.fisKeySpace,
				cassandraConfig.recommendationColFamily,
				recommendationUpdateQuery, cassandraRDD);

		sparkContext.stop();
	}

	/***
	 * 
	 * @param profilePairRDD
	 *            : A PairRDD containing id,regionid and accountid from Profile
	 *            CF.
	 * @param accountPairRDDAllTenant
	 *            : A PairRDD containing accountid and tenantID from Account CF.
	 * @return : A RDD containing userProfile:Record comprised of
	 *         TenantID,RegionID,ProfileID.
	 */

	private JavaRDD<UserProfile> getUserProfile(
			JavaPairRDD<String, String> profilePairRDD,
			JavaPairRDD<String, String> accountPairRDDAllTenant) {
		SparkPIOConnector sparkPIOConnector = new SparkPIOConnector();
		JavaPairRDD<String, String> accountPairRDD = sparkPIOConnector
				.getAccountRecordsforTenant(accountPairRDDAllTenant,
						RecommendationFetcher.TENANT_ID);

		logger.info("Performing join on records read from profile and account CF");
		JavaPairRDD<String, Tuple2<String, String>> profileWithTenantRDD = accountPairRDD
				.join(profilePairRDD);

		logger.info("Creating userProfile for each user");
		JavaRDD<UserProfile> userProfileRDD = sparkPIOConnector
				.getUserProfile(profileWithTenantRDD);
		return userProfileRDD;

	}

	/**
	 * 
	 * This method returns recommendations for provided list of UserProfile from PIO..
	 * 
	 * @param userProfileRDD
	 *            List of users for whom recommendation will be fetched.
	 * @param pioConfig
	 *            Instance of PIOConfig, which holds holds information for PIO
	 *            interactions, like appName,appURL, engineName etc.
	 * @return JavaRDDPair(key-value pair) of recommendations fetched from PIO.
	 *         Key is UserProfile and Value is list of recommendations.
	 */
	private JavaPairRDD<UserProfile, List<String>> getRecommendations(
			JavaRDD<UserProfile> userProfileRDD, PIOConfig pioConfig) {
		SparkPIOConnector sparkPIOConnector = new SparkPIOConnector();

		JavaPairRDD<UserProfile, List<String>> allRecommendationsRDD = sparkPIOConnector
				.getRecommendations(userProfileRDD, pioConfig.appKey,
						pioConfig.appURL, pioConfig.engineName,
						pioConfig.numRecPerUser);

		JavaPairRDD<UserProfile, List<String>> filteredRecommendationsRDD = sparkPIOConnector
				.removeEmptyRecommendations(allRecommendationsRDD);
		return filteredRecommendationsRDD;

	}

}
