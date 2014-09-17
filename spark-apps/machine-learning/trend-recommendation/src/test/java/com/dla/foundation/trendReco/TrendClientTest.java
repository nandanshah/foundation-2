package com.dla.foundation.trendReco;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.trendReco.util.TrendRecommendationUtil;

public class TrendClientTest {
	private TrendRecoClient trendRecoClient;
	private CassandraContext cassandra;
	private String current_dir = null;

	@Before
	public void beforeClass() throws InterruptedException, IOException {
		trendRecoClient = new TrendRecoClient();
		current_dir = System.getProperty("user.dir");
		cassandra = new CassandraContext(current_dir
				+ "/../../commons/src/test/resources/cassandra.yaml");
		cassandra.connect();
		executeCommands();
	}

	@Test
	public void userEvtSummaryCalTest() throws Exception {
		assertNotNull(trendRecoClient);

		trendRecoClient.runTrendRecommendation(current_dir
				+ "/../../commons/src/test/resources/common.properties");

		assertNotNull(cassandra);
		ResultSet dayScoreResult = cassandra.getRows("fistest",
				"common_daily_eventsummary_per_useritem");
		double sum = 0;
		for (Row row : dayScoreResult) {
			try {
				if (row.getDate("date").getTime() == TrendRecommendationUtil
						.getFormattedDate(TrendRecommendationUtil.getDate(
								"2014-06-29", "yyyy-MM-dd").getTime())) {
					sum = sum + row.getDouble("dayscore");
				}
			} catch (ParseException e) {

				e.printStackTrace();
			}
		}
		assertEquals(1.4, sum, 0);

		ResultSet trendScoreResult = cassandra.getRows("fistest", "trend_reco");

		for (Row row : trendScoreResult) {

			if (row.getUUID("itemid").toString() == "c979ca35-b58d-434b-b2d6-ea0316bcc121") {

				assertEquals(1.0607, row.getDouble("trendscore"), 0);
			}
			if (row.getUUID("itemid").toString() == "c979ca35-b58d-434b-b2d6-ea0316bcc122") {

				assertEquals(0.2142, row.getDouble("trendscore"), 0);
			}
		}
	}

	@After
	public void afterClass() throws InterruptedException {
		cassandra.executeCommand("drop keyspace IF EXISTS fistest;");
		cassandra.close();
		Thread.sleep(20000);
	}

	private void executeCommands() {
		try {
			BufferedReader in = new BufferedReader(new FileReader(
					"src/test/resources/trendSchemaCom.txt"));
			String command;
			while ((command = in.readLine()) != null) {
				cassandra.executeCommand(command.trim());
			}
			in.close();
		} catch (IOException ex) {

		}
	}
}
