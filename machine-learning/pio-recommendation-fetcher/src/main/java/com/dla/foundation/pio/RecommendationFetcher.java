package com.dla.foundation.pio;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.pio.util.CassandraConfig;
import com.dla.foundation.pio.util.PIOConfig;
import com.dla.foundation.pio.util.PropKeys;

public class RecommendationFetcher implements Serializable {
	
	private static final long serialVersionUID = 648420875123997020L;
	public static final String DEFAULT_PROPERTIES_FILE_PATH = "src/main/resources/PIO_props.properties";
	public static final String PROPERTIES_FILE_VAR = "propertiesfile";
	public static final String DEFAULT_API_PORT_NUM = "8000";
	private static final String DEFAULT_CASSANDRA_PORT_NUM = "9160";
	private static Logger logger = Logger.getLogger(RecommendationFetcher.class.getName());
	

	/**
	 * This method is initial method for PIO Recommendation Fetcher. It
	 * initializes different config parameters from Property file. And sends
	 * request to fetch recommendations for Users
	 * 
	 * @param propertyHandler
	 *            : instance of PropertyHandler which helps to retrieve values
	 *            for different properties.
	 * @throws IOException 
	 */

	public void runRecommendationFetcher(PropertiesHandler propertyHandler) {
		
		//Calculating PIO appURL
		String port;
		try {
			port = propertyHandler.getValue(PropKeys.PIOPort.getValue()) != null ? propertyHandler.getValue(PropKeys.PIOPort.getValue())
					: DEFAULT_API_PORT_NUM;
		
		final String appURL = "http://" +  propertyHandler.getValue(PropKeys.HOSTNAME.getValue()) + ":" + port;

		// Instantiates PIOConfig Class : This class holds config properties
		// needed to make call to predictionIO (PIO).
		PIOConfig pioConfig = new PIOConfig(propertyHandler.getValue(PropKeys.APPKEY.getValue()),
				appURL,propertyHandler.getValue(PropKeys.ENGINE.getValue()),
				Integer.parseInt(propertyHandler.getValue(PropKeys.NUM_REC_PER_USER.getValue())));
		logger.info("");


		
		// Forms Cassandra update query : This query will be used for inserting
		// fetched recommendation to Cassandra table.
				final String recommendationUpdateQuery = "UPDATE "
				+ propertyHandler.getValue(PropKeys.KEYSPACE.getValue()) + "."
				+ propertyHandler.getValue(PropKeys.RECOMMEND_CF.getValue()) + " SET "
				+ propertyHandler.getValue(PropKeys.RECOMMEND_REC_COL.getValue()) + "=?,"
				+ propertyHandler.getValue(PropKeys.RECOMMEND_TIMESTAMP_COL.getValue()) + "=?";
				
		

		RecommendationFetcherDriver recFetcherDriver = new RecommendationFetcherDriver();
		Configuration conf = new Configuration();

		
				CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
				 getCassnadraIPArray(propertyHandler.getValue(PropKeys.CASSANDRA_IP.getValue())),								
				propertyHandler.getValue(PropKeys.INPUT_PARTITIONER.getValue()),
				propertyHandler.getValue(PropKeys.CASSANDRA_PORT.getValue()),
				getCassnadraIPArray(propertyHandler.getValue(PropKeys.CASSANDRA_IP.getValue())),									
				propertyHandler.getValue(PropKeys.OUTPUT_PARTITIONER.getValue()));
				
				
		// Instantiates CassandraConfig : This class holds parameters that are
		// used for reading and writing data from Cassandra.
		
		CassandraConfig cassandraPIOConfig = new CassandraConfig(
				propertyHandler.getValue(PropKeys.KEYSPACE.getValue()),
				propertyHandler.getValue(PropKeys.PROFILE_CF.getValue()),
				propertyHandler.getValue(PropKeys.PAGE_ROW_SIZE.getValue()),
				propertyHandler.getValue(PropKeys.PROFILE_USERIDKEY.getValue()),
				recommendationUpdateQuery, propertyHandler.getValue(PropKeys.KEYSPACE.getValue()),
				propertyHandler.getValue(PropKeys.RECOMMEND_CF.getValue()),
				propertyHandler.getValue(PropKeys.RECOMMEND_USERIDKEY.getValue()));
		
		

		recFetcherDriver.fetchRecommendations(cassandraSparkConnector, conf,
				cassandraPIOConfig, pioConfig,
				propertyHandler.getValue(PropKeys.SPARK_MASTER.getValue()),
				propertyHandler.getValue(PropKeys.SPARK_APPNAME.getValue()));
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}

	}
	
	
	private String [] getCassnadraIPArray(String strCassnadraIP){
		return strCassnadraIP.split(",");
	}

	public static void main(String[] args) throws IOException {

		RecommendationFetcher recommndationFetcher = new RecommendationFetcher();
		PropertiesHandler propertyHandler = new PropertiesHandler(
				System.getProperty(PROPERTIES_FILE_VAR,
						DEFAULT_PROPERTIES_FILE_PATH));
		recommndationFetcher.runRecommendationFetcher(propertyHandler);

	}

}
