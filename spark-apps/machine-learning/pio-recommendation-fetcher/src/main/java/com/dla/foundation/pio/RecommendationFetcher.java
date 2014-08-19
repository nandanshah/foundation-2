package com.dla.foundation.pio;

import java.io.IOException;
import java.io.Serializable;

import org.apache.log4j.Logger;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.pio.util.CassandraConfig;
import com.dla.foundation.pio.util.PIOConfig;
import com.dla.foundation.pio.util.PropKeys;
import com.dla.foundation.pio.util.RecoFetcherConstants;

public class RecommendationFetcher implements Serializable {

	private static final long serialVersionUID = 648420875123997020L;
	public static String TENANT_ID;
	private static final String CONFIG_DELIM = ",";

	private static Logger logger = Logger.getLogger(RecommendationFetcher.class
			.getName());

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

		// Calculating PIO appURL
		String port;
		try {
			port = propertyHandler.getValue(CommonPropKeys.pio_port.getValue()) != null ? propertyHandler
					.getValue(CommonPropKeys.pio_port.getValue())
					: RecoFetcherConstants.DEFAULT_API_PORT_NUM;

			final String appURL = "http://"
					+ propertyHandler.getValue(CommonPropKeys.pio_host
							.getValue()) + ":" + port;

			// Instantiates PIOConfig Class : This class holds config properties
			// needed to make call to predictionIO (PIO).
			String[] engineConfig = propertyHandler.getValue(TENANT_ID).split(
					CONFIG_DELIM);
			String appKey = engineConfig[0];
			String engineName = engineConfig[1];

			PIOConfig pioConfig = new PIOConfig(appKey, appURL, engineName,
					Integer.parseInt(propertyHandler
							.getValue(PropKeys.NUM_REC_PER_USER.getValue())));

			RecommendationFetcherDriver recFetcherDriver = new RecommendationFetcherDriver();

			CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
					getCassnadraIPArray(propertyHandler.getValue(CommonPropKeys.cs_hostList
							.getValue())),
					RecoFetcherConstants.INPUT_PARTITIONER,
					propertyHandler.getValue(CommonPropKeys.cs_rpcPort
							.getValue()),
					getCassnadraIPArray(propertyHandler
							.getValue(CommonPropKeys.cs_hostList.getValue())),
					RecoFetcherConstants.OUTPUT_PARTITIONER);

			// Instantiates CassandraConfig : This class holds parameters that
			// are
			// used for reading and writing data from Cassandra.

			CassandraConfig cassandraConfig = new CassandraConfig(
					propertyHandler.getValue(CommonPropKeys.cs_analyticsKeyspace
							.getValue()),
					propertyHandler.getValue(CommonPropKeys.cs_fisKeyspace
							.getValue()),
					propertyHandler.getValue(CommonPropKeys.cs_profileCF
							.getValue()),
					propertyHandler.getValue(CommonPropKeys.cs_accountCF
							.getValue()),
					propertyHandler.getValue(PropKeys.RECOMMEND_CF.getValue()),
					propertyHandler.getValue(CommonPropKeys.cs_pageRowSize
							.getValue()));

			recFetcherDriver.fetchRecommendations(cassandraSparkConnector,
					cassandraConfig, pioConfig, propertyHandler
							.getValue(CommonPropKeys.spark_host.getValue()));
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}

	}

	private String[] getCassnadraIPArray(String strCassnadraIP) {
		return strCassnadraIP.split(",");
	}

	public static void main(String[] args) throws IOException {
		String propertiesFilePath = "";

		RecommendationFetcher recommndationFetcher = new RecommendationFetcher();
		if (args.length ==2) {
			propertiesFilePath = args[0];
			TENANT_ID = args[1];
			System.out.println("TENANT ID " + TENANT_ID);
			System.out.println(propertiesFilePath);
			PropertiesHandler propertyHandler = new PropertiesHandler(
					propertiesFilePath, RecoFetcherConstants.APPNAME);
			recommndationFetcher.runRecommendationFetcher(propertyHandler);
		} else {
			System.err
					.println(" USAGE: RecommendationFetcher propertiesfile tenantID");
		}

	}

}
