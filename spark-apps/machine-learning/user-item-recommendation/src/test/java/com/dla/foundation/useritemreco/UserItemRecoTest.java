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
import com.dla.foundation.useritemreco.model.UserItemRecoCF;
import com.dla.foundation.useritemreco.util.UserItemRecommendationUtil;
import com.dla.foundation.analytics.utils.CassandraContext;

public class UserItemRecoTest {
	private CassandraContext cassandra;
	private UserItemRecoDriver userItemRecoDriver;
	private String current_dir;

	@Before
	public void beforeClass() throws InterruptedException, IOException {
		userItemRecoDriver = new UserItemRecoDriver();
		current_dir = System.getProperty("user.dir");
		cassandra = new CassandraContext(current_dir
				+ "/../../commons/src/test/resources/cassandra.yaml");

		cassandra.connect();
		executeCommands();

	}

	@Test
	public void userItemRecoCalulator() throws IOException, ParseException {
		assertNotNull(cassandra);
		assertNotNull(userItemRecoDriver);

		userItemRecoDriver.runUserItemRecoDriver(current_dir
				+ "/../../commons/src/test/resources/common.properties");
		ResultSet userItemResult = cassandra.getRows("fistest",
				"user_item_reco");
		for (Row row : userItemResult) {
			try {
				if (0 == row
						.getUUID(UserItemRecoCF.REGION.getColumn())
						.toString()
						.compareToIgnoreCase(
								"C979CA35-B58D-434B-B2D6-EA0316BCC9A1")
						&& 0 == row
								.getUUID(UserItemRecoCF.TENANT.getColumn())
								.toString()
								.compareToIgnoreCase(
										"C979CA35-B58D-434B-B2D6-EA0316BCC9A1")
						&& 0 == row
								.getUUID(UserItemRecoCF.ITEM.getColumn())
								.toString()
								.compareToIgnoreCase(
										"C979CA35-B58D-434B-B2D6-EA0316BCC9A1")
						&& 0 == row
								.getUUID(UserItemRecoCF.PROFILE.getColumn())
								.toString()
								.compareToIgnoreCase(
										"9769e61f-238f-11b2-7f7f-7f7f7f7f7f7f")
						&& UserItemRecommendationUtil
								.getFormattedDate(UserItemRecommendationUtil
										.getDate("2014-06-30", "yyyy-MM-dd")
										.getTime()) == row.getDate(
								UserItemRecoCF.DATE.getColumn()).getTime()) {

					assertEquals(0.6,
							row.getDouble(UserItemRecoCF.POPULARITY_SCORE
									.getColumn()), 0);
					assertEquals(0.6, row.getDouble(UserItemRecoCF.TREND_SCORE
							.getColumn()), 0);
					assertEquals(0.6,
							row.getDouble(UserItemRecoCF.NEW_RELEASE_SCORE
									.getColumn()), 0);
					assertEquals(0.6, row.getDouble(UserItemRecoCF.SOCIAL_SCORE
							.getColumn()), 0);
					assertEquals(
							0.6,
							row.getDouble(UserItemRecoCF.PIO_SCORE.getColumn()),
							0);
				}

				if (0 == row
						.getUUID(UserItemRecoCF.REGION.getColumn())
						.toString()
						.compareToIgnoreCase(
								"C979CA35-B58D-434B-B2D6-EA0316BCC9A1")
						&& 0 == row
								.getUUID(UserItemRecoCF.TENANT.getColumn())
								.toString()
								.compareToIgnoreCase(
										"C979CA35-B58D-434B-B2D6-EA0316BCC9A1")
						&& 0 == row
								.getUUID(UserItemRecoCF.ITEM.getColumn())
								.toString()
								.compareToIgnoreCase(
										"C979CA35-B58D-434B-B2D6-EA0316BCC9A1")
						&& 0 == row
								.getUUID(UserItemRecoCF.PROFILE.getColumn())
								.toString()
								.compareToIgnoreCase(
										"C979CA35-B58D-434B-B2D6-EA0316BCC9A1")
						&& UserItemRecommendationUtil
								.getFormattedDate(UserItemRecommendationUtil
										.getDate("2014-06-30", "yyyy-MM-dd")
										.getTime()) == row.getDate(
								UserItemRecoCF.DATE.getColumn()).getTime()) {

					assertEquals(0.6,
							row.getDouble(UserItemRecoCF.POPULARITY_SCORE
									.getColumn()), 0);
					assertEquals(0.6, row.getDouble(UserItemRecoCF.TREND_SCORE
							.getColumn()), 0);
					assertEquals(0.6,
							row.getDouble(UserItemRecoCF.NEW_RELEASE_SCORE
									.getColumn()), 0);
					assertEquals(0.6, row.getDouble(UserItemRecoCF.SOCIAL_SCORE
							.getColumn()), 0);
					assertEquals(
							0.6,
							row.getDouble(UserItemRecoCF.PIO_SCORE.getColumn()),
							0);
				}
			} catch (ParseException e) {
				e.printStackTrace();
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
