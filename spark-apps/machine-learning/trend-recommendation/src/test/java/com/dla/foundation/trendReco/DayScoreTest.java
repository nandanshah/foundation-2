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



public class DayScoreTest {
	private DayScoreDriver dayScoreDriver;
	private CassandraContext cassandra;
	
	@Before
	public void beforeClass() throws InterruptedException, IOException {
		dayScoreDriver = new DayScoreDriver();
		
		String current_dir = "file://" + System.getProperty("user.dir");
		cassandra = new CassandraContext(current_dir
				+ "/../../commons/src/test/resources/cassandra.yaml");
		
		cassandra.connect();
		executeCommands();
	}
	
	@Test
	public void dayScoreCalTest() {
		assertNotNull(dayScoreDriver);
		dayScoreDriver
				.runDayScoreDriver(
						"src/test/resources/appPropTest.txt",
						"src/test/resources/dayScorePropTest.txt");
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
							"src/test/resources/dayScoreCom.txt"));
			String command;
			while ((command = in.readLine()) != null) {
				cassandra.executeCommand(command.trim());
			}
			in.close();
		} catch (IOException ex) {

		}
	}
}
