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

public class UserEvtSummaryTest {
	private UserEventSummaryDriver userEventSummaryDriver;
	private CassandraContext cassandra;

	@Before
	public void beforeClass() throws InterruptedException {
		userEventSummaryDriver = new UserEventSummaryDriver();
		cassandra = new CassandraContext();
		cassandra.connect();
		executeCommands();
	}

	@Test
	public void userEvtSummaryCalTest() {
		assertNotNull(userEventSummaryDriver);
		userEventSummaryDriver
				.runUserEvtSummaryDriver(
						"src/test/resources/appPropTest.txt",
						"src/test/resources/userSumPropTest.txt");
		assertNotNull(cassandra);
		ResultSet dayScoreResult = cassandra.getRows("sampletrendrecotest3",
				"dailyeventsummaryperuseritem");
		for (Row row : dayScoreResult) {
			try {
				if (row.getDate("date").getTime() == TrendRecommendationUtil
						.getFormattedDate(TrendRecommendationUtil.getDate(
								"2014-06-29", "yyyy-MM-dd").getTime())) {

					assertEquals(0.6, row.getDouble("dayscore"), 0);
				}
			} catch (ParseException e) {

				e.printStackTrace();
			}

		}

	}

	@After
	public void afterClass() throws InterruptedException {
		cassandra.executeCommand("drop keyspace IF EXISTS sampletrendrecotest3;");
		cassandra.close();
		Thread.sleep(20000);
	}

	private void executeCommands() {
		try {
			BufferedReader in = new BufferedReader(
					new FileReader(
							"src/test/resources/userSumCom.txt"));
			String command;
			while ((command = in.readLine()) != null) {
				cassandra.executeCommand(command.trim());
			}
			in.close();
		} catch (IOException ex) {

		}
	}

}
