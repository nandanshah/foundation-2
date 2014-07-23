package com.dla.foundation.trendReco;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dla.foundation.analytics.utils.CassandraContext;

public class TrendScoreTest {
	private TrendScoreDriver trendScoreDriver;
	private CassandraContext cassandra;

	@Before
	public void beforeClass() throws InterruptedException {
		trendScoreDriver = new TrendScoreDriver();
		cassandra = new CassandraContext();
		cassandra.connect();
		executeCommands();
	}

	@Test
	public void userEvtSummaryCalTest() {
		assertNotNull(trendScoreDriver);
		trendScoreDriver
				.runTrendScoreDriver(
						"src/main/resources/local/appPropTest",
						"src/main/resources/local/trendScorePropTest");
		assertNotNull(cassandra);
		ResultSet dayScoreResult = cassandra
				.getRows("sampletrendrecotest2", "trend");
		for (Row row : dayScoreResult) {

			if (row.getInt("tenantid") == 1 && row.getInt("regionid") == 1
					&& row.getInt("itemid") == 122) {

				assertEquals(1.6, row.getDouble("trendscore"), 0.1);
				assertEquals(1, row.getDouble("normalizedscore"), 0);
			}

		}

	}

	@After
	public void afterClass() throws InterruptedException {
		cassandra.executeCommand("drop keyspace IF EXISTS sampletrendrecotest2;");
		cassandra.close();
		Thread.sleep(20000);
	}

	private void executeCommands() {
		try {
			BufferedReader in = new BufferedReader(
					new FileReader(
							"src/main/resources/local/trendScoreCom.txt"));
			String command;
			while ((command = in.readLine()) != null) {
				cassandra.executeCommand(command.trim());
			}
			in.close();
		} catch (IOException ex) {

		}
	}

}
