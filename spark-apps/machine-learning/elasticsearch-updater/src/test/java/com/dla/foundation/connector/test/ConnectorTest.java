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
/*
 * Test class to validate the Cassandra to ES application flow and logic.
 */
public class ConnectorTest {
	
	private CassandraContext cassandra;
	private CassandraEntityReader reader =null;
	private DeleteESType deleteTypes = null;
	private ElasticSearchRepo repository= null;
	final private static String esHost = LocalElasticSearch.elasticSearchBaseUrl;
	private String catalogIndex = "catalog",reco_type ="reco_type",reco_type_id = "_show",mapping = "_mapping";
	
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
	
	/*
	 * Test method to validate and verify cassandra to ES flow.
	 */
	@Test
	public void userRecoTest() throws IOException, InterruptedException, ParseException {
	    deleteTypes= new DeleteESType();
		reader = new CassandraEntityReader();
		String current_dir = System.getProperty("user.dir");
		ESWriter.init(current_dir+"/../../commons/src/test/resources/common.properties","src/main/resources/");
		
		RecoType recotype = repository.getItem(catalogIndex,reco_type,reco_type_id);
		
		//As it is created if not present so it cannot be null
		assertNotNull(recotype);
		
		//Delete the passive reco type before inserting records into it.
		deleteTypes.deleteType(current_dir+"/../../commons/src/test/resources/common.properties");
		boolean typePresent =repository.checkESIndexTypeIfExists(catalogIndex+ESWriter.reco_type.getPassive()+mapping, ESWriter.esHost);
		
		//As type is deleted, it should not be present in index.
		assertEquals(typePresent,false);
		reader.runUserRecoDriver(current_dir+"/../../commons/src/test/resources/common.properties");	
		
	}
	
	/*
	 * Execute cassandra commands stored in file(src\test\resources\cassandraCommands.txt) to 
	 * create required column families and insert few records in it. 
	 */
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
	
	@AfterClass
	public static void afterClass() throws Exception {
		try {
			LocalElasticSearch.cleanup();
		} catch (Exception ex) {
			System.out.println("Error cleaning up: " + ex.getMessage());
		}
	}
}
