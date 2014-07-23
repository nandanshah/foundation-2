package com.dla.foundation.test;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dla.foundation.ESMovieSimilarity;
import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.analytics.utils.PropertiesHandler;

public class ESMovieSimilarityTest {

	CassandraContext cassandraInstance;
	PropertiesHandler cassandraProperties;

	@Before
	public void createMockCassandra() throws InterruptedException, IOException {

		cassandraProperties = new PropertiesHandler(
				"src/main/resources/local/Cassandra.properties");
		cassandraInstance = new CassandraContext();
		cassandraInstance.connect();
		cassandraInstance
				.executeCommand("CREATE KEYSPACE IF NOT EXISTS "
						+ cassandraProperties.getValue("keyspace")
						+ " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 1}");
		cassandraInstance.executeCommand("use "
				+ cassandraProperties.getValue("keyspace"));
		cassandraInstance.executeCommand("CREATE TABLE IF NOT EXISTS "
				+ cassandraProperties.getValue("columnFamily") + " ("
				+ cassandraProperties.getValue("primaryKey")
				+ " TEXT PRIMARY KEY , "
				+ cassandraProperties.getValue("recoField") + " LIST<TEXT> )");
	}

	@Test
	public void runTest() throws IOException {
		ESMovieSimilarity.launchApp();
		ResultSet results = cassandraInstance.getRows("catalog",
				"similarmovies");
		List<Text> appResult = new ArrayList<Text>();
		Map<Text, List<String>> expectedResults = new HashMap<Text, List<String>>();
		BufferedReader resFile = new BufferedReader(new FileReader(
				"src/test/resources/expectedResults.txt"));
		String expRecord;
		while ((expRecord = resFile.readLine()) != null) {
			String[] temp = expRecord.split("=");
			String key = temp[0];
			String value = temp[1];
			List<String> valueList = new ArrayList<String>();
			for (String s : value.split(",")) {
				valueList.add(s.trim());
			}
			expectedResults.put(new Text(key.trim()), valueList);
		}

		boolean testPassed = true;

		for (Row row : results.all()) {
			String key = row.getString(cassandraProperties
					.getValue("primaryKey"));
			List<String> movieReco = row.getList(
					cassandraProperties.getValue("recoField"), String.class);
			if (!expectedResults.get(new Text(key)).containsAll(movieReco))
				testPassed = false;
		}
		assertTrue(testPassed);
	}
}
