package com.dla.foundation.crawler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.crawler.util.CrawlerPropKeys;

public class CrawlerTest {

	static CassandraContext context;
	public static final String propertisFilePath = "src/test/resources/crawler_test.properties";
	public static final String commandsFile = "src/test/resources/crawlercommands.txt";
	private static String socialProfileCF, friendsCF, keySpace;

	@BeforeClass
	public static void beforeTest() throws IOException, InterruptedException {
		initCassandra();
	}

	private static void initCassandra() throws IOException,
			InterruptedException {
		PropertiesHandler phandler = null;
		try {
			phandler = new PropertiesHandler(propertisFilePath);
		} catch (IOException e) {
			System.err.println("Error getting properties file");
			throw e;
		}

		keySpace = phandler.getValue(CrawlerPropKeys.fisKeyspace.getValue());

		socialProfileCF = phandler
				.getValue(CrawlerPropKeys.socialProfileColumnFamily.getValue());

		friendsCF = phandler.getValue(CrawlerPropKeys.friendsColumnFamily
				.getValue());

		String current_dir = "file://" + System.getProperty("user.dir");
		context = new CassandraContext(current_dir
				+ "/../../commons/src/test/resources/cassandra.yaml");
		context.connect();
		// FIXME: this is very specific test case as i'm using my user id and my
		// friends user id for testing. Will need some dummy accounts here
		executeCommands(context, commandsFile);
	}

	@Test
	public void testCrawler() throws IOException {

		String columnName = "username";
		long outdatedTime = 1402857000002L;

		CrawlerDriver driver = new CrawlerDriver();
		driver.run(propertisFilePath, outdatedTime);

		String outputUsername = getUserName(socialProfileCF, columnName);
		Assert.assertTrue("Username must not be null", outputUsername != null);
		int numFriends = getFriendsCount(friendsCF);
		Assert.assertTrue("Number of Friends must be greater than one",
				numFriends > 0);
	}

	private int getFriendsCount(String columnFamily) {
		ResultSet results = context.getRows(keySpace, columnFamily);
		return results.all().size();
	}

	private String getUserName(String columnFamily, String colName) {
		ResultSet results = context.getRows(keySpace, columnFamily);
		Row row = results.one();
		if (row == null)
			return null;
		String opUserName = row.getString(colName);
		return opUserName;
	}

	@AfterClass
	public static void afterTest() {
		context.close();
	}

	private static void executeCommands(CassandraContext context,
			String commandsFile) {
		try {
			BufferedReader in = new BufferedReader(new FileReader(commandsFile));
			String command;
			while ((command = in.readLine()) != null) {
				command = command.trim();
				if (!command.equalsIgnoreCase("")) {
					System.out.println("Executing Command :" + command);
					context.executeCommand(command.trim());
				}
			}
			in.close();
		} catch (IOException ex) {

		}
	}
}
