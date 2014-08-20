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
	public void beforeClass() throws InterruptedException, IOException {
		trendScoreDriver = new TrendScoreDriver();
		
		String current_dir = "file://" + System.getProperty("user.dir");
		cassandra = new CassandraContext(current_dir
				+ "/../../commons/src/test/resources/cassandra.yaml");
		
		cassandra.connect();
		executeCommands();
	}

	@Test
	public void TrendScoreCalTest() {
		assertNotNull(trendScoreDriver);
		trendScoreDriver
				.runTrendScoreDriver(
						"src/test/resources/appPropTest.txt",
						"src/test/resources/trendScorePropTest.txt");
		assertNotNull(cassandra);
		ResultSet dayScoreResult = cassandra
				.getRows("sampletrendrecotest2", "trend");
		for (Row row : dayScoreResult) {

			if (0==row.getUUID("tenantid").toString().compareTo("c979ca35-b58d-434b-b2d6-ea0316bcc9a1") && 0==row.getUUID("regionid").toString().compareTo("c979ca35-b58d-434b-b2d6-ea0316bcc9a1")
					&& 0==row.getUUID("itemid").toString().compareTo("c979ca35-b58d-434b-b2d6-ea0316bcc122")) {

				assertEquals(1.6, row.getDouble("trendscore"), 0.1);
				assertEquals(1, row.getDouble("normalizedscore"), 0);
				assertEquals("trending", row.getString("trendscorereason").toLowerCase());
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
							"src/test/resources/trendScoreCom.txt"));
			String command;
			while ((command = in.readLine()) != null) {
				cassandra.executeCommand(command.trim());
			}
			in.close();
		} catch (IOException ex) {

		}
	}

}
