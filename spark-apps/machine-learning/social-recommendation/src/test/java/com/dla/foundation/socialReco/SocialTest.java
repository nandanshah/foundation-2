package com.dla.foundation.socialReco;

import static org.junit.Assert.*;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.ResultSet;
import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.socialReco.SocialScoreDriver;

public class SocialTest {

	private SocialScoreDriver driver;
	private CassandraContext cassandra;
	private String current_dir = null;

	@Before
	public void beforeClass() throws Exception {

		driver = new SocialScoreDriver();
		current_dir = System.getProperty("user.dir");
		cassandra = new CassandraContext(current_dir
				+ "/../../commons/src/test/resources/cassandra.yaml");
		cassandra.connect();
		executeCommands();

		assertNotNull(driver);
		driver.run(current_dir 
				+ "/../../commons/src/test/resources/common.properties");
	}

	@Test
	public void SocialTest() throws Exception {

		assertNotNull(cassandra);

		countTest();

		ResultSet socialRecoResult = cassandra.getRows("fistest", "social_reco");

		ResultSet sparkAppPropResult = cassandra.getRows("fistest",
				"eo_spark_app_prop");

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

		Map<String, String> appPropMap = new HashMap<String, String>();

		for (Row approw : sparkAppPropResult) {
			appPropMap = approw.getMap("properties", String.class, String.class);
		}

		if (appPropMap.get("social_reco_date_flag").toString().equals("true"))
		{

			for (Row row : socialRecoResult) 
			{
					assertEquals("Date and social_reco_date should be same",formatter.format(row.getDate("date")).toString(),appPropMap.get("social_reco_date").toString());

			}
		}
		else 
		{

			for (Row row : socialRecoResult)
			{
					assertEquals("Recalculation end date and social date should be same",formatter.format(row.getDate("date")).toString(),appPropMap.get("recalculation_end_date").toString());

			}

		}

	}


	public void countTest()
	{
		int count=0;

		ResultSet socialRecoResult = cassandra.getRows("fistest", "social_reco");
		for (Row row : socialRecoResult)
		{
			count++;
		}

		assertEquals("count should be equal to 10 ",10, count);

	}


	private void executeCommands() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					"src/test/resources/socialRecoCommands.txt"));
			String command;
			while ((command = reader.readLine()) != null) {
				cassandra.executeCommand(command.trim());
			}
			reader.close();

		} catch (IOException ex) {

			ex.printStackTrace();
		}
	}

}
