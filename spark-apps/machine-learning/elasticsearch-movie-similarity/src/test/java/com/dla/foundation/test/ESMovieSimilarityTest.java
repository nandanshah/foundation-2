package com.dla.foundation.test;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dla.foundation.CassandraWriter;
import com.dla.foundation.ESSparkConnector;
import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.analytics.utils.PropertiesHandler;

public class ESMovieSimilarityTest {

	CassandraContext cassandraInstance;
	PropertiesHandler cassandraProperties;

	public void createMockCassandra() throws InterruptedException, IOException {

		String current_dir = System.getProperty("user.dir");
		cassandraInstance = new CassandraContext(current_dir
				+ "/../../commons/src/test/resources/cassandra.yaml");
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

	@Before
	public void init() throws IOException, InterruptedException {

		JobConf conf = new JobConf();
		PropertiesHandler esProperties = null;
		PropertiesHandler sparkProperties = null;
		try {
			// Read ElasticSearch properties
			esProperties = new PropertiesHandler(
					"src/test/resources/ES.properties");
			// Read Cassandra Properties

			cassandraProperties = new PropertiesHandler(
					"src/test/resources/Cassandra.properties");
			sparkProperties = new PropertiesHandler(
					"src/test/resources/Spark.properties");
		} catch (IOException e) {
			System.out
					.println("Unable to read properties file. Exception:" + e);
			System.exit(0);
		}
		createMockCassandra();
		ESSparkConnector con = new ESSparkConnector(
				sparkProperties.getValue("sparkMaster"),
				sparkProperties.getValue("sparkAppName"),
				sparkProperties.getValue("port"),
				esProperties.getValue("serverAddress"),
				esProperties.getValue("transportPort"),
				esProperties.getValue("httpPort"),
				esProperties.getValue("index"), esProperties.getValue("type"),
				esProperties.getValue("clusterName"),
				cassandraProperties.getValue("keyspace"),
				cassandraProperties.getValue("columnFamily"),
				cassandraProperties.getValue("primaryKey"));

		// Get all movies
		JavaPairRDD<Text, MapWritable> allMovies = con.getAllMovies();
		// Find Similar Movies
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> similarMovies = con
				.getSimilarMovies(allMovies);
		// Write results to cassandra.
		String[] cassandraIP = cassandraProperties.getValue("cassandraServer")
				.split(",");
		CassandraWriter cassandraCon = new CassandraWriter(conf, cassandraIP,
				cassandraProperties.getValue("port"),
				cassandraProperties.getValue("keyspace"),
				cassandraProperties.getValue("columnFamily"),
				cassandraProperties.getValue("primaryKey"),
				cassandraProperties.getValue("columnFamilyPartitioner"),
				cassandraProperties.getValue("recoField"));
		cassandraCon.writeToCassandra(similarMovies);

	}

	@Test
	public void runTest() throws IOException {
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
