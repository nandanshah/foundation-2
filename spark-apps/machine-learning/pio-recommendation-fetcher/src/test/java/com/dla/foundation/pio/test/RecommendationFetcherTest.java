package com.dla.foundation.pio.test;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.pio.RecommendationFetcher;
import com.dla.foundation.pio.util.PropKeys;

public class RecommendationFetcherTest {
	public static final String DEFAULT_USERS_FILE_PATH = "src/test/resources/users.csv";
	public static final String DEFAULT_PROPERTIES_FILE_PATH = "src/test/resources/PIO_props.properties";
	public static final String DEFAULT_ITEMS_FILE_PATH = "src/test/resources/movieData.csv";
	public static final String DEFAULT_BEHAVIOUR_FILE_PATH = "src/test/resources/activity.csv";
	public static final String DEFAULT_PREFETCHED_USER_FILE_PATH = "src/test/resources/userlist.csv";
	private static PropertiesHandler propertyHandler;
	public static final String DEFAULT_API_PORT_NUM = "8000";
	public CassandraContext cassandraContext;

	public static final String FILE_VAR = "propertiesfile";
	private static Logger logger = Logger
			.getLogger(RecommendationFetcherTest.class.getName());

	@Before
	public void setUp() throws Exception {
		propertyHandler = new PropertiesHandler(System.getProperty(FILE_VAR,
				DEFAULT_PROPERTIES_FILE_PATH));

		String current_dir = "file://" + System.getProperty("user.dir");
		cassandraContext = new CassandraContext(current_dir
				+ "/../../commons/src/test/resources/cassandra.yaml");
		cassandraContext.connect();

		
		cassandraContext
				.executeCommand("CREATE KEYSPACE IF NOT EXISTS "
						+ propertyHandler.getValue(PropKeys.KEYSPACE.getValue())
						+ " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		logger.info("Created keyspace "
				+ propertyHandler.getValue(PropKeys.KEYSPACE.getValue()));
		System.out.println("Created keyspace "
				+ propertyHandler.getValue(PropKeys.KEYSPACE.getValue()));
		cassandraContext.executeCommand("use "
				+ propertyHandler.getValue(PropKeys.KEYSPACE.getValue()));
		logger.info("Using keyspace "
				+ propertyHandler.getValue(PropKeys.KEYSPACE.getValue()));
		System.out.println("Using keyspace "
				+ propertyHandler.getValue(PropKeys.KEYSPACE.getValue()));

		cassandraContext
				.executeCommand("CREATE TABLE IF NOT EXISTS "
						+ propertyHandler.getValue(PropKeys.PROFILE_CF
								.getValue())
						+ " ( accountid uuid, id uuid, avatarimageurl text,birthdate timestamp,defaultcolor text,emailaddress text,emailaddress_lower text,firstname text,gender text,isadmin boolean,isrootprofile boolean,lastname text,notfiyfriendregistration boolean,notifyfriendlike boolean,notifynewrelease boolean,notifypromotional boolean,notifywatchlistexpiration boolean,passwordhash text,passwordsalt text,pinhash text,pinsalt text,profiletype text,socialauthtoken text,watchlist_id uuid,PRIMARY KEY (accountid, id));");

		logger.info("Created columnfamily : "
				+ propertyHandler.getValue(PropKeys.PROFILE_CF.getValue()));

		cassandraContext
				.executeCommand("CREATE TABLE IF NOT EXISTS "
						+ propertyHandler.getValue(PropKeys.RECOMMEND_CF
								.getValue())
						+ " ( profileid uuid,recommendations list<uuid>,lastupdate timestamp, PRIMARY KEY (profileid));");
		logger.info("Created columnfamily : "
				+ propertyHandler.getValue(PropKeys.RECOMMEND_CF.getValue()));
		ImportData dataImporter = new ImportData(propertyHandler);

		for (String userID : dataImporter.readUsers(
				DEFAULT_PREFETCHED_USER_FILE_PATH, ",")) {
			String query = "INSERT INTO "
					+ propertyHandler.getValue(PropKeys.PROFILE_CF.getValue())
					+ "(accountid,id) VALUES (" + userID + "," + userID + ")";
			cassandraContext.executeCommand(query);

		}
		
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		Scanner inputReader = new Scanner(System.in);
		RecommendationFetcher recFetcher = new RecommendationFetcher();
		try {
			logger.info("Started spark context for fetching recommendations from PIO.");
			recFetcher.runRecommendationFetcher(propertyHandler);
			logger.info("Completed fetching recommendations from PIO.");
			cassandraContext.executeCommand("use "
					+ propertyHandler.getValue(PropKeys.KEYSPACE.getValue()));
			logger.info("Using keyspace "
					+ propertyHandler.getValue(PropKeys.KEYSPACE.getValue()));
			ResultSet recommendationsResult = cassandraContext.getRows(
					propertyHandler.getValue(PropKeys.KEYSPACE.getValue()),
					propertyHandler.getValue(PropKeys.RECOMMEND_CF.getValue()));
			inputReader.close();

			assertTrue(compareData(recommendationsResult));
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	public boolean compareData(ResultSet rs) {

		List<String> userList = new ArrayList<String>();
		userList.add("c3daa4b0-faa9-11e3-8143-19ddb396c99f");
		userList.add("c3dacbc4-faa9-11e3-8143-19ddb396c99f");
		userList.add("c3dacbc5-faa9-11e3-8143-19ddb396c99f");
		userList.add("c3dacbcc-faa9-11e3-8143-19ddb396c99f");
		userList.add("c3dacbc9-faa9-11e3-8143-19ddb396c99f");
		String[] recArray = { "f4794c90-045c-11e4-aa10-e75e4a96a3ee",
				"f53fa9d0-045c-11e4-aa10-e75e4a96a3ee",
				"f56d7090-045c-11e4-aa10-e75e4a96a3ee",
				"f6addfd0-045c-11e4-aa10-e75e4a96a3ee",
				"f4c597d0-045c-11e4-aa10-e75e4a96a3ee",
				"f55e2e50-045c-11e4-aa10-e75e4a96a3ee",
				"f502a0d0-045c-11e4-aa10-e75e4a96a3ee",
				"f5d84050-045c-11e4-aa10-e75e4a96a3ee",
				"f4a71350-045c-11e4-aa10-e75e4a96a3ee",
				"f5b9bbd0-045c-11e4-aa10-e75e4a96a3ee" };
		List<String> recList = Arrays.asList(recArray);
		List<String> cassandraUserList = new ArrayList<String>();
		for (Row userRecord : rs.all()) {
			String uid = userRecord.getUUID("profileid").toString();
			List<UUID> fetchedRecUUID = userRecord.getList("recommendations",
					UUID.class);
			cassandraUserList.add(uid);
			List<String> fetchedRec = new ArrayList<String>();
			for (UUID movie : fetchedRecUUID) {
				fetchedRec.add(movie.toString());
			}
			if (!fetchedRec.containsAll(recList)
					&& fetchedRec.size() == recList.size()) {
				return false;
			}

		}
		if (cassandraUserList.containsAll(userList)
				&& cassandraUserList.size() == userList.size()) {
			System.out.println("Test Case is Pass");
			return true;
		}

		return false;

	}

}
