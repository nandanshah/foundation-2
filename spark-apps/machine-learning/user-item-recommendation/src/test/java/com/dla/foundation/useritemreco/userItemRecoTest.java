package com.dla.foundation.useritemreco;

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
import com.dla.foundation.useritemreco.model.userItemRecoCF;
import com.dla.foundation.useritemreco.util.UserItemRecommendationUtil;
import com.dla.foundation.analytics.utils.CassandraContext;

public class userItemRecoTest {
	private CassandraContext cassandra;
	private UserItemRecoDriver userItemRecoDriver;

	@Before
	public void beforeClass() throws InterruptedException, IOException {
		userItemRecoDriver = new UserItemRecoDriver();
		
		
		String current_dir = "file://" + System.getProperty("user.dir");
		cassandra = new CassandraContext(current_dir
				+ "/../../commons/src/test/resources/cassandra.yaml");
		
		cassandra.connect();
		executeCommands();

	}

	@Test
	public void userItemRecoCalulator() {
		assertNotNull(cassandra);
		assertNotNull(userItemRecoDriver);
		userItemRecoDriver.runUserItemRecoDriver("src/test/resources/appProp",
				"src/test/resources/scoreSummaryProp.txt",
				"src/test/resources/userItemSummaryProp.txt");
		ResultSet userItemResult = cassandra.getRows("useritemrecotest",
				"useritemrecommendation");
		for (Row row : userItemResult) {
			try {
				if (0 == row.getUUID(userItemRecoCF.REGION.getColumn())
						.toString()
						.compareTo("C979CA35-B58D-434B-B2D6-EA0316BCC9A1")
						&& 0 == row
								.getUUID(userItemRecoCF.TENANT.getColumn())
								.toString()
								.compareTo(
										"C979CA35-B58D-434B-B2D6-EA0316BCC9A1")
						&& 0 == row
								.getUUID(userItemRecoCF.ITEM.getColumn())
								.toString()
								.compareTo(
										"C979CA35-B58D-434B-B2D6-EA0316BCC9A1")
						&& 0 == row
								.getUUID(userItemRecoCF.USER_ID.getColumn())
								.toString()
								.compareTo(
										"C979CA35-B58D-434B-B2D6-EA0316BCC9A1")
						&& UserItemRecommendationUtil
								.getFormattedDate(UserItemRecommendationUtil
										.getDate("2014-06-30", "yyyy-MM-dd")
										.getTime()) == row.getDate(
								userItemRecoCF.DATE.getColumn()).getTime()) {

					assertEquals(0.6,
							row.getDouble(userItemRecoCF.POPULARITY_SCORE
									.getColumn()), 0);
					assertEquals(0.6, row.getDouble(userItemRecoCF.TREND_SCORE
							.getColumn()), 0);
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	@After
	public void afterClass() throws InterruptedException {
		cassandra.executeCommand("drop keyspace IF EXISTS useritemrecotest;");
		cassandra.close();
		Thread.sleep(20000);
	}

	private void executeCommands() {
		try {
			BufferedReader in = new BufferedReader(new FileReader(
					"src/test/resources/userItemRecoCommand.txt"));
			String command;
			while ((command = in.readLine()) != null) {
				System.out.println(command.trim());
				cassandra.executeCommand(command.trim());
			}
			in.close();
		} catch (IOException ex) {

		}
	}
}
