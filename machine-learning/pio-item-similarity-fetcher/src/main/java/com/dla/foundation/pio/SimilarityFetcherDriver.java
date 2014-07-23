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
import com.dla.foundation.pio.util.CassandraConfig;
import com.dla.foundation.pio.util.PIOConfig;

public class SimilarityFetcherDriver implements Serializable {

	private static final long serialVersionUID = 2894456804103519801L;
	private static Logger logger = Logger
			.getLogger(SimilarityFetcherDriver.class.getName());

	/**
	 * This method reads items from cassandra table,fetches item similarity from
	 * PIO and saves results to Cassandra table.
	 * 
	 * @param cassandraSparkConnector
	 *            Instance of CassandraSparkConnector to save results to
	 *            Cassandra table.
	 * @param conf
	 *            Instance of Configuration
	 * @param cassandraConfig
	 *            Instance of CassandraConfig, which holds information for
	 *            Cassandra read / write like Cassandra
	 *            keySpace,columnFamily,primary-key etc.
	 * @param pioConfig
	 *            Instance of PIOConfig, which holds holds information for PIO
	 *            interactions, like appName,appURL, engineName etc.
	 * @param sparkAppMaster
	 *            Config parameter Master for instantiating SparkContext.
	 * @param sparkAppName
	 *            Config parameter AppName for instantiating SparkContext.
	 */

	public void fetchSimilarity(
			CassandraSparkConnector cassandraSparkConnector,
			Configuration conf, CassandraConfig cassandraConfig,
			PIOConfig pioConfig, String sparkAppMaster, String sparkAppName) {

		SparkPIOConnector sparkPIOConnector = new SparkPIOConnector();
		// Instantiates JavaSparkContext
		logger.info("Initializing java spark context");
		JavaSparkContext sparkContext = sparkPIOConnector
				.initilizeSparkContext(sparkAppMaster, sparkAppName);
		logger.info("Initialized java spark context successfully");
		// Reads items from Cassandra table.
		logger.info("Reading item records from Cassandra");
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> recordsFromCassandra = cassandraSparkConnector
				.read(conf, sparkContext, cassandraConfig.sourceKeySpace,
						cassandraConfig.sourceColFamily,
						cassandraConfig.pageRowSize);

		// Retrives itemid from Data read from Cassandra.
		logger.info("Retriving itemids from items records");
		JavaRDD<String> allItemsRDD = sparkPIOConnector
				.getItemsFromCassandraRecords(recordsFromCassandra,
						cassandraConfig);

		
		JavaPairRDD<String, List<String>> fetchedItemSimilarities = getItemSimilarity(
				sparkContext, allItemsRDD, pioConfig);
		// Converts Primary Keys to Map<String, ByteBuffer> and Other values to
		// java.util.List<ByteBuffer>
		JavaPairRDD<Map<String, ByteBuffer>, java.util.List<ByteBuffer>> cassandraRDD = sparkPIOConnector
				.toCassandraRDDforSavingDataToCassandraTable(
						fetchedItemSimilarities,
						cassandraConfig.destinationPrimaryKey);
		logger.info("Writing item similarities to Cassandra");
		// Saves results to Cassandra.
		cassandraSparkConnector.write(conf,
				cassandraConfig.destinationKeySpace,
				cassandraConfig.destinationColFamily,
				cassandraConfig.destinationUpdateQuery, cassandraRDD);

		sparkContext.stop();
	}

	/**
	 * 
	 * This method returns item similarities for provided list of itemids from PIO..
	 * 
	 * @param sparkContext
	 *            Instance of JavaSparkContext.
	 * @param allItemsRDD
	 *            List of items for whom similar items will be fetched.
	 * @param pioConfig
	 *            Instance of PIOConfig, which holds holds information for PIO
	 *            interactions, like appName,appURL, engineName etc.
	 * @return JavaRDDPair(key-value pair) of item similarities fetched from PIO.
	 *         Key is itemid and Value is list of similar items.
	 */
	private JavaPairRDD<String, List<String>> getItemSimilarity(
			JavaSparkContext sparkContext, JavaRDD<String> allItemsRDD,
			PIOConfig pioConfig) {
		SparkPIOConnector sparkPIOConnector = new SparkPIOConnector();

		JavaPairRDD<String, List<String>> similarityForAllItems = sparkPIOConnector
				.getItemSimilarityFromPIO(allItemsRDD, pioConfig.appKey,
						pioConfig.appURL, pioConfig.engineName,
						pioConfig.numRecPerItem);

		JavaPairRDD<String, List<String>> filteredSimilarityRDD = sparkPIOConnector
				.removeEmptySimilarityRecords(similarityForAllItems);
		return filteredSimilarityRDD;

	}

}
