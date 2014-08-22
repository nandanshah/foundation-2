package com.dla.foundation.trendReco;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.trendReco.util.PropKeys;
import com.dla.foundation.trendReco.util.TrendRecommendationUtil;

public class TrendRecoClient {

	private static final String IP_SEPARATOR = ",";
	private static final Logger logger = Logger
			.getLogger(TrendRecoClient.class);

	public void runTrendRecommendation(String appPropFilePath,
			String userSumPropFilePath, String dayScorePropFilePath,
			String trendScorePropPath) throws Exception {
	
			logger.info("Initializing property handler ");
			// Initializing property handler
			PropertiesHandler appProp = new PropertiesHandler(appPropFilePath);
			// initializing spark context
			logger.info("initializing spark context");
			JavaSparkContext sparkContext = new JavaSparkContext(
					appProp.getValue(PropKeys.MODE_PROPERTY.getValue()),
					appProp.getValue(PropKeys.APP_NAME.getValue()));

			// initializing spark-cassandra connector
			logger.info("initializing spark-cassandra connector");
			CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
					TrendRecommendationUtil.getList(appProp
							.getValue(PropKeys.INPUT_HOST_LIST.getValue()),
							IP_SEPARATOR),
					appProp.getValue(PropKeys.INPUT_PARTITIONER.getValue()),
					appProp.getValue(PropKeys.INPUT_RPC_PORT.getValue()),
					TrendRecommendationUtil.getList(appProp
							.getValue(PropKeys.OUTPUT_HOST_LIST.getValue()),
							","), appProp.getValue(PropKeys.OUTPUT_PARTITIONER
							.getValue()));
			UserEventSummaryDriver userSummaryDriver = new UserEventSummaryDriver();
			userSummaryDriver.runUserEvtSummaryDriver(sparkContext,
					cassandraSparkConnector, userSumPropFilePath);
			DayScoreDriver dayScoreDriver = new DayScoreDriver();
			dayScoreDriver.runDayScoreDriver(sparkContext,
					cassandraSparkConnector, dayScorePropFilePath);
			TrendScoreDriver trendScoreDriver = new TrendScoreDriver();
			trendScoreDriver.runTrendScoreDriver(sparkContext,
					cassandraSparkConnector, trendScorePropPath);
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 4) {
			TrendRecoClient client = new TrendRecoClient();
			client.runTrendRecommendation(args[0], args[1], args[2], args[3]);
		} else {
			System.err.println("Provide the property file paths");
		}
	}

}
