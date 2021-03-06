package com.dla.foundation.pio;

import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.pio.util.CassandraConfig;
import com.dla.foundation.pio.util.PIOConfig;
import com.dla.foundation.pio.util.RecoFetcherConstants;

public class RecommendationFetcher implements Serializable {

	private static final long serialVersionUID = 648420875123997020L;
	public static String TENANT_ID;
	public static Date RECO_FETCH_TIMESTAMP;
	private static final String CONFIG_DELIM = ",";
	private static final String POST_FIX_TO_TENANT = ":last_processed";
	private static final String HTTP_PREFIX="http://";
	private static final String PORT_DELIM = ":";
	
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

	public void runRecommendationFetcher(PropertiesHandler propertyHandler)
			throws IOException {

		String port;

		RECO_FETCH_TIMESTAMP = new Date(getFormattedDate(System.currentTimeMillis()));

		logger.info(RecoFetcherConstants.APPNAME
				+ " started fetching recommendations for tenantid "
				+ RecommendationFetcher.TENANT_ID + " at "
				+ RECO_FETCH_TIMESTAMP);
		// Forming PIO appURL
		port = propertyHandler.getValue(CommonPropKeys.pio_port.getValue()) != null ? propertyHandler
				.getValue(CommonPropKeys.pio_port.getValue())
				: RecoFetcherConstants.DEFAULT_API_PORT_NUM;

		final String appURL = HTTP_PREFIX
				+ propertyHandler.getValue(CommonPropKeys.pio_host.getValue())
				+ PORT_DELIM + port;

		// Instantiates PIOConfig Class : This class holds config properties
		// needed to make call to predictionIO (PIO).
		String[] engineConfig = propertyHandler.getValue(TENANT_ID).split(
				CONFIG_DELIM);
		String appKey = engineConfig[0];
		String engineName = engineConfig[1];

		PIOConfig pioConfig = new PIOConfig(appKey, appURL, engineName,
				Integer.parseInt(propertyHandler
						.getValue(RecoFetcherConstants.PIO_NUM_REC_PER_USER)));

		RecommendationFetcherDriver recFetcherDriver = new RecommendationFetcherDriver();

		CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
				getCassnadraIPArray(propertyHandler.getValue(CommonPropKeys.cs_hostList
						.getValue())), RecoFetcherConstants.INPUT_PARTITIONER,
				propertyHandler.getValue(CommonPropKeys.cs_rpcPort.getValue()),
				getCassnadraIPArray(propertyHandler
						.getValue(CommonPropKeys.cs_hostList.getValue())),
				RecoFetcherConstants.OUTPUT_PARTITIONER);

		// Instantiates CassandraConfig : This class holds parameters that
		// are
		// used for reading and writing data from Cassandra.

		CassandraConfig cassandraConfig = new CassandraConfig(
				propertyHandler.getValue(CommonPropKeys.cs_platformKeyspace
						.getValue()),
				propertyHandler.getValue(CommonPropKeys.cs_fisKeyspace
						.getValue()),
				propertyHandler.getValue(CommonPropKeys.cs_profileCF.getValue()),
				propertyHandler.getValue(CommonPropKeys.cs_accountCF.getValue()),
				RecoFetcherConstants.RECOMMEND_CF,
				propertyHandler.getValue(CommonPropKeys.cs_pageRowSize
						.getValue()));

		recFetcherDriver.fetchRecommendations(cassandraSparkConnector,
				cassandraConfig, pioConfig,
				propertyHandler.getValue(CommonPropKeys.spark_host.getValue()));
		propertyHandler.writeToCassandra(TENANT_ID.concat(POST_FIX_TO_TENANT),RECO_FETCH_TIMESTAMP.toString());
	}

	private String[] getCassnadraIPArray(String strCassnadraIP) {
		return strCassnadraIP.split(CONFIG_DELIM);
	}
	

	public static long getFormattedDate(long time) {
		Calendar date = new GregorianCalendar();
		date.setTimeInMillis(time);
		date.set(Calendar.HOUR_OF_DAY, 0);
		date.set(Calendar.MINUTE, 0);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MILLISECOND, 0);
		return date.getTimeInMillis();
	}

	public static void main(String[] args) throws IOException {
		String propertiesFilePath = "";
		try {
			RecommendationFetcher recommndationFetcher = new RecommendationFetcher();
			if (args.length == 2) {
				propertiesFilePath = args[0];
				TENANT_ID = args[1];
				PropertiesHandler propertyHandler = new PropertiesHandler(
						propertiesFilePath, RecoFetcherConstants.APPNAME);
				recommndationFetcher.runRecommendationFetcher(propertyHandler);
			} else {
				logger.error(" Please provide valid arguments : propertiesfile tenantID");
				throw new IllegalArgumentException(
						" Please provide valid arguments : propertiesfile tenantID");
			}
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			throw new IOException(e);
		}
	}

}
