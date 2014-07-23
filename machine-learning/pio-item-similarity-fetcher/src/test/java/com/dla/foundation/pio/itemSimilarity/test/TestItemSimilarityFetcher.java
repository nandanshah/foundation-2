package com.dla.foundation.pio.itemSimilarity.test;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.pio.SimilarityFetcher;
import com.dla.foundation.pio.util.PropKeys;
import com.dla.foundation.pio.itemSimilarity.test.ImportData;

import junit.framework.TestCase;

public class TestItemSimilarityFetcher extends TestCase {
	private static Map<String, List<String>> preDataMap;
	public static final String DEFAULT_USERS_FILE_PATH = "src/test/resources/users.csv";
	public static final String DEFAULT_PROPERTIES_FILE_PATH = "src/test/resources/PIO_props.properties";
	public static final String DEFAULT_ITEMS_FILE_PATH = "src/test/resources/movieData.csv";
	public static final String DEFAULT_BEHAVIOUR_FILE_PATH = "src/test/resources/activity.csv";
	public static final String DEFAULT_PREFETCHED_USER_FILE_PATH = "src/test/resources/userlist.csv";
	private static PropertiesHandler propertyHandler;
	public CassandraContext cassandraContext;
	public static final String FILE_VAR = "propertiesfile";
	private static Logger logger = Logger
			.getLogger(TestItemSimilarityFetcher.class.getName());

	@Before
	protected void setUp() throws Exception {
		preDataMap = new HashMap<String, List<String>>();
		initPreDataMap();
		propertyHandler = new PropertiesHandler(System.getProperty(FILE_VAR,
				DEFAULT_PROPERTIES_FILE_PATH));

		cassandraContext = new CassandraContext();
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

		cassandraContext.executeCommand("CREATE TABLE IF NOT EXISTS "
				+ propertyHandler.getValue(PropKeys.SOURCE_CF.getValue())
				+ " ( id uuid,PRIMARY KEY (id));");

		logger.info("Created columnfamily : "
				+ propertyHandler.getValue(PropKeys.SOURCE_CF.getValue()));

		cassandraContext
				.executeCommand("CREATE TABLE IF NOT EXISTS "
						+ propertyHandler.getValue(PropKeys.DESTINATION_CF
								.getValue())
						+ " ( itemid uuid,"+propertyHandler.getValue(PropKeys.DESTINATION_SIM_COL.getValue()) +" list<uuid>,lastupdate timestamp, PRIMARY KEY (itemid));");
		logger.info("Created columnfamily : "
				+ propertyHandler.getValue(PropKeys.DESTINATION_CF.getValue()));
		ResultSet similarityResults = cassandraContext.getRows(
				propertyHandler.getValue(PropKeys.KEYSPACE.getValue()),
				propertyHandler.getValue(PropKeys.DESTINATION_CF.getValue()));

		ImportData dataImporter = new ImportData(propertyHandler);

		for (String itemid : dataImporter.readUsers(
				DEFAULT_ITEMS_FILE_PATH, ",")) {
			String query = "INSERT INTO "
					+ propertyHandler.getValue(PropKeys.SOURCE_CF.getValue())
					+ "(id) VALUES (" + itemid + ")";
			logger.info("Inserting test data to "
					+ propertyHandler.getValue(PropKeys.SOURCE_CF.getValue())
					+ " : " + query);
			cassandraContext.executeCommand(query);

		}
	}

	@Test
	public void testRunSimilarityFetcher() {
		SimilarityFetcher recFetcher = new SimilarityFetcher();
		try {
			logger.info("Started spark context for fetching similarities from PIO.");
			recFetcher.runSimilarityFetcher(propertyHandler);
			logger.info("Completed fetching similarities from PIO.");
			cassandraContext.executeCommand("use "
					+ propertyHandler.getValue(PropKeys.KEYSPACE.getValue()));
			logger.info("Using keyspace "
					+ propertyHandler.getValue(PropKeys.KEYSPACE.getValue()));
			ResultSet similarityResults = cassandraContext.getRows(
					propertyHandler.getValue(PropKeys.KEYSPACE.getValue()),
					propertyHandler.getValue(PropKeys.DESTINATION_CF.getValue()));
			
			assertTrue(compareResults(similarityResults));
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public static boolean compareResults(ResultSet similarityResults) throws IOException{
		for(Row record : similarityResults.all()){
			String itemid = record.getUUID(propertyHandler.getValue(PropKeys.DESTINATION_PK.getValue())).toString();
			List<UUID> fetchedSimilarityUUID = record.getList(propertyHandler.getValue(PropKeys.DESTINATION_SIM_COL.getValue()),
					UUID.class);
			List<String> fetchedSimilaritiesString = new ArrayList<String>();
			for(UUID item: fetchedSimilarityUUID){
				fetchedSimilaritiesString.add(item.toString());
			}
			if(!fetchedSimilaritiesString.containsAll(preDataMap.get(itemid))){
				System.out.println(fetchedSimilarityUUID.containsAll(preDataMap.get(itemid))+"False AT : "+itemid +"List 1:");
				System.out.println(fetchedSimilarityUUID.toString()+ "List 2: ");
				System.out.println(preDataMap.get(itemid));
				
				return false;
			}
		}
		
		return true;
	}

	public static void initPreDataMap() {
		String list1[] = new String[] { "f4a71350-045c-11e4-aa10-e75e4a96a3ee",
				"f55e2e50-045c-11e4-aa10-e75e4a96a3ee",
				"f56d7090-045c-11e4-aa10-e75e4a96a3ee",
				"f502a0d0-045c-11e4-aa10-e75e4a96a3ee" };
		String list2[] = new String[] { "f55e2e50-045c-11e4-aa10-e75e4a96a3ee",
				"f6addfd0-045c-11e4-aa10-e75e4a96a3ee",
				"f4a71350-045c-11e4-aa10-e75e4a96a3ee",
				"f5b9bbd0-045c-11e4-aa10-e75e4a96a3ee",
				"f4794c90-045c-11e4-aa10-e75e4a96a3ee",
				"f56d7090-045c-11e4-aa10-e75e4a96a3ee" };
		String list3[] = new String[] { "f502a0d0-045c-11e4-aa10-e75e4a96a3ee",
				"f5d84050-045c-11e4-aa10-e75e4a96a3ee",
				"f4794c90-045c-11e4-aa10-e75e4a96a3ee",
				"f56d7090-045c-11e4-aa10-e75e4a96a3ee",
				"f4a71350-045c-11e4-aa10-e75e4a96a3ee",
				"f55e2e50-045c-11e4-aa10-e75e4a96a3ee" };
		String list4[] = new String[] { "f6addfd0-045c-11e4-aa10-e75e4a96a3ee",
				"f4a71350-045c-11e4-aa10-e75e4a96a3ee",
				"f55e2e50-045c-11e4-aa10-e75e4a96a3ee",
				"f502a0d0-045c-11e4-aa10-e75e4a96a3ee",
				"f5d84050-045c-11e4-aa10-e75e4a96a3ee",
				"f56d7090-045c-11e4-aa10-e75e4a96a3ee" };
		String list5[] = new String[] { "f4a71350-045c-11e4-aa10-e75e4a96a3ee",
				"f55e2e50-045c-11e4-aa10-e75e4a96a3ee",
				"f6addfd0-045c-11e4-aa10-e75e4a96a3ee",
				"f502a0d0-045c-11e4-aa10-e75e4a96a3ee",
				"f4794c90-045c-11e4-aa10-e75e4a96a3ee",
				"f5d84050-045c-11e4-aa10-e75e4a96a3ee",
				"f5b9bbd0-045c-11e4-aa10-e75e4a96a3ee" };
		String list6[] = new String[] { "f6addfd0-045c-11e4-aa10-e75e4a96a3ee",
				"f4a71350-045c-11e4-aa10-e75e4a96a3ee",
				"f4794c90-045c-11e4-aa10-e75e4a96a3ee",
				"f56d7090-045c-11e4-aa10-e75e4a96a3ee" };
		String list7[] = new String[] { "f5d84050-045c-11e4-aa10-e75e4a96a3ee",
				"f5b9bbd0-045c-11e4-aa10-e75e4a96a3ee",
				"f56d7090-045c-11e4-aa10-e75e4a96a3ee",
				"f502a0d0-045c-11e4-aa10-e75e4a96a3ee",
				"f4794c90-045c-11e4-aa10-e75e4a96a3ee",
				"f6addfd0-045c-11e4-aa10-e75e4a96a3ee",
				"f55e2e50-045c-11e4-aa10-e75e4a96a3ee" };
		String list8[] = new String[] { "f502a0d0-045c-11e4-aa10-e75e4a96a3ee",
				"f5b9bbd0-045c-11e4-aa10-e75e4a96a3ee",
				"f4794c90-045c-11e4-aa10-e75e4a96a3ee",
				"f56d7090-045c-11e4-aa10-e75e4a96a3ee",
				"f4a71350-045c-11e4-aa10-e75e4a96a3ee",
				"f6addfd0-045c-11e4-aa10-e75e4a96a3ee"

		};

		preDataMap.put("f5b9bbd0-045c-11e4-aa10-e75e4a96a3ee",
				Arrays.asList(list1));
		preDataMap.put("f502a0d0-045c-11e4-aa10-e75e4a96a3ee",
				Arrays.asList(list2));
		preDataMap.put("f6addfd0-045c-11e4-aa10-e75e4a96a3ee",
				Arrays.asList(list3));
		preDataMap.put("f4794c90-045c-11e4-aa10-e75e4a96a3ee",
				Arrays.asList(list4));
		preDataMap.put("f56d7090-045c-11e4-aa10-e75e4a96a3ee",
				Arrays.asList(list5));
		preDataMap.put("f5d84050-045c-11e4-aa10-e75e4a96a3ee",
				Arrays.asList(list6));
		preDataMap.put("f4a71350-045c-11e4-aa10-e75e4a96a3ee",
				Arrays.asList(list7));
		preDataMap.put("f55e2e50-045c-11e4-aa10-e75e4a96a3ee",
				Arrays.asList(list8));
	}

}
