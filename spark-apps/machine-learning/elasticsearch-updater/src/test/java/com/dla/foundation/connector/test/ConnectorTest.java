package com.dla.foundation.connector.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.connector.data.cassandra.CassandraEntityReader;
import com.dla.foundation.connector.model.RecoType;
import com.dla.foundation.connector.model.UserRecommendation;
import com.dla.foundation.connector.persistence.elasticsearch.DeleteESType;
import com.dla.foundation.connector.persistence.elasticsearch.ESWriter;
import com.dla.foundation.connector.persistence.elasticsearch.ElasticSearchRepo;

public class ConnectorTest {
	
	private CassandraContext cassandra;
	private CassandraEntityReader reader =null;
	private DeleteESType deleteTypes = null;
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
	public void userRecoTest() throws IOException, InterruptedException, ParseException {
	    deleteTypes= new DeleteESType();
		reader = new CassandraEntityReader();
		String current_dir = System.getProperty("user.dir");
		ESWriter.init(current_dir+"/../../commons/src/test/resources/common.properties","src/main/resources/");
		
		RecoType rt = repository.getItem("catalog","reco_type","_show");
		assertNotNull(rt);//As it is created if not present
		
		deleteTypes.deleteType(current_dir+"/../../commons/src/test/resources/common.properties");
		reader.runUserRecoDriver(current_dir+"/../../commons/src/test/resources/common.properties");	
		
		//UserRecommendation ur= repository.getUserRecoItem("d979ca35-b58d-434b-b2d6-ea0316bcc9a6", ESWriter.reco_type.getActive(), "c979ca35-b58d-434b-b2d6-ea0316bcc9a9-c979ca35-b58d-434b-b2d6-ea0316bcc9a8", "c979ca35-b58d-434b-b2d6-ea0316bcc9a9");
		//assertNotNull(ur);
		//assertEquals(ur.getmediaItemId(), "c979ca35-b58d-434b-b2d6-ea0316bcc9a9");
		
	}
	
	private void executeCommands() {
		try {
			BufferedReader in = new BufferedReader(	new FileReader("src/test/resources/cassandraCommands.txt"));
			String command;
			while ((command = in.readLine()) != null) {
				System.out.println(command);
				cassandra.executeCommand(command.trim());
			}
			in.close();
		} catch (IOException ex) {

		}
	} 
	private void updateDateInCommandsFile(){
		BufferedReader in;
		try {
			in = new BufferedReader(	new FileReader("src/test/resources/cassandraCommands.txt"));
			String command;
			while ((command = in.readLine()) != null) {
			
			}
	    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
