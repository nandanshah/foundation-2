package com.dla.foundation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;

import com.dla.foundation.analytics.utils.PropertiesHandler;

public class ESMovieSimilarity {

	public static void main(String[] args) {
		try {
			launchApp();
		} catch (IOException e) {
			System.out.println("Error in reading Properties");
			System.exit(0);
		}
	}

	public static void launchApp() throws IOException {
		JobConf conf = new JobConf();

		// Read ElasticSearch properties
		PropertiesHandler esProperties = new PropertiesHandler(
				"src/main/resources/ES.properties");
		// Read Cassandra Properties
		PropertiesHandler cassandraProperties = new PropertiesHandler(
				"src/main/resources/Cassandra.properties");
		// Read Spark properties
		PropertiesHandler sparkProperties = new PropertiesHandler(
				"src/main/resources/Spark.properties");
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
}
