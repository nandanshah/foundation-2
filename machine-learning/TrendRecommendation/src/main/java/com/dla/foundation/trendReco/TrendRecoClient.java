package com.dla.foundation.trendReco;

public class TrendRecoClient {

	public void runTrendRecommendation(String appPropFilePath,
			String userSummPropFilePath, String dayScorePropFilePath,
			String trendScorePropPath) {
		UserEventSummaryDriver userSummaryDriver = new UserEventSummaryDriver();
		DayScoreDriver dayScoreDriver = new DayScoreDriver();
		TrendScoreDriver trendScoreDriver = new TrendScoreDriver();
		userSummaryDriver.runUserEvtSummaryDriver(appPropFilePath,
				userSummPropFilePath);
		dayScoreDriver.runDayScoreDriver(appPropFilePath, dayScorePropFilePath);

		trendScoreDriver.runTrendScoreDriver(appPropFilePath,
				trendScorePropPath);

	}

	public static void main(String[] args) {
		if (args.length == 4) {
			TrendRecoClient client = new TrendRecoClient();
			client.runTrendRecommendation(args[0], args[1], args[2], args[3]);
		} else {
			System.err.println("Provide the property file paths");
		}
	}

}
