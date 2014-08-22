package com.dla.foundation.trendReco;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.trendReco.util.TrendRecoProp;
import com.dla.foundation.trendReco.util.TrendRecommendationUtil;

public class TrendRecoClient {

	private static final String IP_SEPARATOR = ",";
	private static final Logger logger = Logger
			.getLogger(TrendRecoClient.class);

	public void runTrendRecommendation(String commPropFilePath)
			throws Exception {

		logger.info("Initializing property handler ");
		// Initializing property handler
		PropertiesHandler userEventProp = new PropertiesHandler(
				commPropFilePath, TrendRecoProp.USER_EVENT_SUM_APP_NAME);
		PropertiesHandler dayScoreProp = new PropertiesHandler(
				commPropFilePath, TrendRecoProp.DAILY_EVENT_SUMMARY_APP_NAME);
		PropertiesHandler trendScoreProp = new PropertiesHandler(
				commPropFilePath, TrendRecoProp.TREND_SCORE_APP_NAME);

		// initializing spark context
		logger.info("initializing spark context");
		JavaSparkContext sparkContext = new JavaSparkContext(
				userEventProp.getValue(CommonPropKeys.spark_host.getValue()),
				TrendRecoProp.TREND_RECO_APP_NAME); 

		// initializing spark-cassandra connector
		logger.info("initializing spark-cassandra connector");
		CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
				TrendRecommendationUtil.getList(userEventProp
						.getValue(CommonPropKeys.cs_hostList.getValue()),
						IP_SEPARATOR), TrendRecoProp.PARTITIONER,
				userEventProp.getValue(CommonPropKeys.cs_rpcPort.getValue()),
				TrendRecommendationUtil.getList(userEventProp
						.getValue(CommonPropKeys.cs_hostList.getValue()), ","),
				TrendRecoProp.PARTITIONER);

		UserEventSummaryDriver userSummaryDriver = new UserEventSummaryDriver();
		userSummaryDriver.runUserEvtSummaryDriver(sparkContext,
				cassandraSparkConnector, userEventProp);

		DayScoreDriver dayScoreDriver = new DayScoreDriver();
		dayScoreDriver.runDayScoreDriver(sparkContext, cassandraSparkConnector,
				dayScoreProp);

		TrendScoreDriver trendScoreDriver = new TrendScoreDriver();
		trendScoreDriver.runTrendScoreDriver(sparkContext,
				cassandraSparkConnector, trendScoreProp);

	}

	public static void main(String[] args) throws Exception {
		if (args.length == 1) {
			TrendRecoClient client = new TrendRecoClient();
			client.runTrendRecommendation(args[0]);
		} else {
			System.err.println("Provide the property file paths");
		}
	}
}