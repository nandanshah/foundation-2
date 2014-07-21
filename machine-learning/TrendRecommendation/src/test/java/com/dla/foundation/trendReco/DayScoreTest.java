package com.dla.foundation.trendReco;

import static org.junit.Assert.*;

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



public class DayScoreTest {
	private DayScoreDriver dayScoreDriver;
	private CassandraContext cassandra;
	
	@Before
	public void beforeClass() throws InterruptedException {
		dayScoreDriver = new DayScoreDriver();
		cassandra = new CassandraContext();
		cassandra.connect();
		executeCommands();
	}
	
	@Test
	public void userEvtSummaryCalTest() {
		assertNotNull(dayScoreDriver);
		dayScoreDriver
				.runDayScoreDriver(
						"src/main/resources/appPropTest",
						"src/main/resources/dayScorePropTest");
		assertNotNull(cassandra);
		ResultSet dayScoreResult = cassandra.getRows("sampletrendrecotest1",
				"dailyeventsummary");
		for (Row row : dayScoreResult) {
			try {
				if (row.getDate("date").getTime() == TrendRecommendationUtil
						.getFormattedDate(TrendRecommendationUtil.getDate(
								"2014-06-30", "yyyy-MM-dd").getTime())) {

					assertEquals(3.4, row.getDouble("dayscore"), 0);
				}
			} catch (ParseException e) {

				e.printStackTrace();
			}

		}

	}
	
	@After
	public void afterClass() throws InterruptedException {
		cassandra.executeCommand("drop keyspace IF EXISTS sampletrendrecotest1;");
		cassandra.close();
		Thread.sleep(20000);
	}
	private void executeCommands() {
		try {
			BufferedReader in = new BufferedReader(
					new FileReader(
							"src/main/resources/dayScoreCom.txt"));
			String command;
			while ((command = in.readLine()) != null) {
				cassandra.executeCommand(command.trim());
			}
			in.close();
		} catch (IOException ex) {

		}
	}
}
