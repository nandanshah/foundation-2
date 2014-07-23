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

public class SimilarityFetcher implements Serializable {
	
	private static final long serialVersionUID = 648420875123997020L;
	public static final String DEFAULT_PROPERTIES_FILE_PATH = "src/main/resources/PIO_props.properties";
	public static final String PROPERTIES_FILE_VAR = "propertiesfile";
	public static final String DEFAULT_API_PORT_NUM = "8000";
	private static final String DEFAULT_CASSANDRA_PORT_NUM = "9160";
	private static Logger logger = Logger.getLogger(SimilarityFetcher.class.getName());
	

	/**
	 * This method reads all configurable properties from Property Files and initializes PIOConfig and CassandraConfig
	 *  
	 * @param propertyHandler: instance of PropertyHandler which helps to retrieve values
	 *            for different properties.
	 */

	public void runSimilarityFetcher(PropertiesHandler propertyHandler) {
		
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
				Integer.parseInt(propertyHandler.getValue(PropKeys.NUM_REC_PER_ITEM.getValue())));
		logger.info("Instantited PIOConfig with provided details of PredictionIO Engine");


		
		// Forms Cassandra update query : This query will be used for inserting
		// fetched similarities to Cassandra table.
				final String similarityUpdateQuery = "UPDATE "
				+ propertyHandler.getValue(PropKeys.KEYSPACE.getValue()) + "."
				+ propertyHandler.getValue(PropKeys.DESTINATION_CF.getValue()) + " SET "
				+ propertyHandler.getValue(PropKeys.DESTINATION_SIM_COL.getValue()) + "=?,"
				+ propertyHandler.getValue(PropKeys.DESTINATION_TIMESTAMP_COL.getValue()) + "=?";
				
		

		SimilarityFetcherDriver similarityFetcherDriver = new SimilarityFetcherDriver();
		Configuration conf = new Configuration();

		
				CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
				 getCassnadraIPArray(propertyHandler.getValue(PropKeys.CASSANDRA_IP.getValue())),								
				propertyHandler.getValue(PropKeys.INPUT_PARTITIONER.getValue()),
				propertyHandler.getValue(PropKeys.CASSANDRA_PORT.getValue()),
				getCassnadraIPArray(propertyHandler.getValue(PropKeys.CASSANDRA_IP.getValue())),									
				propertyHandler.getValue(PropKeys.OUTPUT_PARTITIONER.getValue()));
				
				
		// Instantiates CassandraConfig : This class holds parameters that are
		// used for reading and writing data from and to Cassandra respectively.
		
		CassandraConfig cassandraPIOConfig = new CassandraConfig(
				propertyHandler.getValue(PropKeys.KEYSPACE.getValue()),
				propertyHandler.getValue(PropKeys.SOURCE_CF.getValue()),
				propertyHandler.getValue(PropKeys.PAGE_ROW_SIZE.getValue()),
				propertyHandler.getValue(PropKeys.SOURCE_KEY.getValue()),
				similarityUpdateQuery, propertyHandler.getValue(PropKeys.KEYSPACE.getValue()),
				propertyHandler.getValue(PropKeys.DESTINATION_CF.getValue()),
				propertyHandler.getValue(PropKeys.DESTINATION_PK.getValue()));
		
		

		similarityFetcherDriver.fetchSimilarity(cassandraSparkConnector, conf,
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

		SimilarityFetcher similarityFetcher = new SimilarityFetcher();
		PropertiesHandler propertyHandler = new PropertiesHandler(
				System.getProperty(PROPERTIES_FILE_VAR,
						DEFAULT_PROPERTIES_FILE_PATH));
		similarityFetcher.runSimilarityFetcher(propertyHandler);

	}

}
