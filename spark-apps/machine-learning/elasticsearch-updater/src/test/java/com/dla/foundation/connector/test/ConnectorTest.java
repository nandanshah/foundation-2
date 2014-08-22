package com.dla.foundation.connector.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.connector.data.cassandra.CassandraEntityReader;
import com.dla.foundation.connector.model.UserRecommendation;
import com.dla.foundation.connector.persistence.elasticsearch.ElasticSearchRepo;

public class ConnectorTest {
	
	private CassandraContext cassandra;
	private CassandraEntityReader reader =null;
	private ElasticSearchRepo repository= null;
	final private static String esHost = LocalElasticSearch.elasticSearchBaseUrl;
	
	@Before
	public void beforeClass() throws Exception {
		LocalElasticSearch.init();
		repository = new ElasticSearchRepo(esHost);
		
		String current_dir = System.getProperty("user.dir");
		cassandra = new CassandraContext(current_dir
				+ "/../../commons/src/test/resources/cassandra.yaml");
	    cassandra.connect();
		executeCommands();
	}
	
	@Test
	public void userRecoTest() throws IOException, InterruptedException {
		
		reader = new CassandraEntityReader();
		reader.runUserRecoDriver("src/test/resources/appTest.properties", "src/test/resources/userRecoTest.properties", "src/test/resources/elasticSearchTest.properties");	
		
		boolean exists= repository.createESIndex("d979ca35-b58d-434b-b2d6-ea0316bcc9b7", esHost);
		assertEquals(exists, true);
		
		UserRecommendation ur= repository.getUserRecoItem("d979ca35-b58d-434b-b2d6-ea0316bcc9a6", "user_reco_1", "c979ca35-b58d-434b-b2d6-ea0316bcc9a9-c979ca35-b58d-434b-b2d6-ea0316bcc9a8", "c979ca35-b58d-434b-b2d6-ea0316bcc9a9");
		assertNotNull(ur);
		assertEquals(ur.getParentItemId(), "c979ca35-b58d-434b-b2d6-ea0316bcc9a9");
		
	}
	
	private void executeCommands() {
		try {
			BufferedReader in = new BufferedReader(	new FileReader("src/test/resources/cassandraCommands.txt"));
			String command;
			while ((command = in.readLine()) != null) {
				cassandra.executeCommand(command.trim());
			}
			in.close();
		} catch (IOException ex) {

		}
	} 
	
	@AfterClass
	public static void afterClass() throws Exception {
		try {
			LocalElasticSearch.cleanup();
		} catch (Exception ex) {
			System.out.println("Error cleaning up: " + ex.getMessage());
		}
	}
}
