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

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.pio.util.CassandraConfig;
import com.dla.foundation.pio.util.PIOConfig;

public class RecommendationFetcherDriver implements Serializable {

	private static final long serialVersionUID = 2894456804103519801L;
	private static Logger logger = Logger.getLogger(RecommendationFetcherDriver.class.getName());
	

	/**
	 * This method fetches recommendations from PIO and
	 * save results to Cassandra table.
	 * 
	 * @param cassandraSparkConnector
	 *            Instance of CassandraSparkConnector to save results to
	 *            Cassandra table.
	 * @param conf
	 *            Instance of Configuration
	 * @param cassandraConfig
	 *            Instance of CassandraPIOConfig, which holds information for
	 *            Cassandra write, like Cassandra
	 *            keySpace,columnFamily,primary-key etc.
	 * @param pioConfig
	 *            Instance of PIOConfig, which holds holds information for PIO
	 *            interactions, like appName,appURL, engineName etc.
	 * @param sparkAppMaster
	 *            Config parameter Master for instantiating SparkContext.
	 * @param sparkAppName
	 *            Config parameter AppName for instantiating SparkContext.
	 */

	public void fetchRecommendations(
			CassandraSparkConnector cassandraSparkConnector,
			Configuration conf, CassandraConfig cassandraConfig,
			PIOConfig pioConfig, String sparkAppMaster, String sparkAppName) {

		SparkPIOConnector sparkPIOConnector = new SparkPIOConnector();
		// Instantiates JavaSparkContext
		logger.info("Initializing java spark context");
		JavaSparkContext sparkContext = sparkPIOConnector
				.initilizeSparkContext(sparkAppMaster, sparkAppName);
		logger.info("Initialized java spark context successfully");
		//Reads users from Cassandra table.
		logger.info("Reading user records from Cassandra");
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> recordsFromCassandra = cassandraSparkConnector.read(conf, sparkContext, cassandraConfig.profileKeySpace, cassandraConfig.profileColFamily, cassandraConfig.pageRowSize);
								
		
		//Retrives userid from Data retrived from Cassandra.
		logger.info("Retriving userids from user records");
		JavaRDD<String> allUsersRDD = sparkPIOConnector.getUsersFromCassandraRecords(
				recordsFromCassandra, cassandraConfig);
		
		//Get recommendation for Users read from Cassandra from PIO
		JavaPairRDD<String, List<String>> pioRecommendations = getRecommendationsForUsers(
				sparkContext, allUsersRDD, pioConfig);
		
		//Converts Primary Keys to Map<String, ByteBuffer> and Other values to java.util.List<ByteBuffer> 
		JavaPairRDD<Map<String, ByteBuffer>, java.util.List<ByteBuffer>> cassandraRDD = sparkPIOConnector
				.formatRecommendationsForCassandraWrite(pioRecommendations,
						cassandraConfig.recommendationPrimaryKey);
		logger.info("Writting user recommendations to Cassandra");
		//Saves results to Cassandra.
		cassandraSparkConnector.write(conf, cassandraConfig.recommendationKeySpace, cassandraConfig.recommendationColFamily, cassandraConfig.recommendationUpdateQuery, cassandraRDD);
		
				
		
		sparkContext.stop();
	}

	/**
	 * 
	 * This method returns recommendations for provided list of users from PIO..
	 * 
	 * @param sparkContext
	 *            Instance of JavaSparkContext.
	 * @param allUsersRDD
	 *            List of users for whom recommendation will be fetched.
	 * @param pioConfig
	 *            Instance of PIOConfig, which holds holds information for PIO
	 *            interactions, like appName,appURL, engineName etc.
	 * @return JavaRDDPair(key-value pair) of recommendations fetched from PIO.
	 *         Key is UserID and Value is list of recommendations.
	 */
	private JavaPairRDD<String, List<String>> getRecommendationsForUsers(
			JavaSparkContext sparkContext, JavaRDD<String> allUsersRDD,
			PIOConfig pioConfig) {
		SparkPIOConnector sparkPIOConnector = new SparkPIOConnector();
		
		JavaPairRDD<String, List<String>> allRecommendations = sparkPIOConnector
				.getRecommendationsFromPIO(allUsersRDD,
						pioConfig.appKey, pioConfig.appURL,
						pioConfig.engineName, pioConfig.numRecPerUser);

		JavaPairRDD<String, List<String>> filteredRecommendations = sparkPIOConnector
				.removeEmptyRecommendations(allRecommendations);
		return filteredRecommendations;

	}

		

}
