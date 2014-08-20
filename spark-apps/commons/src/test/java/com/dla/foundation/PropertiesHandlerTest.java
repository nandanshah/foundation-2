package com.dla.foundation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;

public class PropertiesHandlerTest {

	private PropertiesHandler handler, pHandler;
	private CassandraContext csContext;

	@Before
	public void before() throws InterruptedException, IOException {

		try {
			handler = new PropertiesHandler(
					"src/test/resources/common.properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		String current_dir = "file://" + System.getProperty("user.dir");
		csContext = new CassandraContext(current_dir
				+ "/src/test/resources/cassandra.yaml");

		csContext.connect();

		csContext
				.executeCommand("CREATE KEYSPACE IF NOT EXISTS "
						+ handler.getValue(CommonPropKeys.cs_fisKeyspace)
						+ " WITH replication={'class':'SimpleStrategy','replication_factor':1};");
		csContext.executeCommand("USE "
				+ handler.getValue(CommonPropKeys.cs_fisKeyspace));
		csContext.executeCommand("CREATE TABLE IF NOT EXISTS "
				+ handler.getValue(CommonPropKeys.cs_sparkAppPropCF) + "("
				+ handler.getValue(CommonPropKeys.cs_sparkAppPropCol)
				+ " text, properties map<text,text>, primary key ("
				+ handler.getValue(CommonPropKeys.cs_sparkAppPropCol) + "));");
		csContext.executeCommand("INSERT INTO "
				+ handler.getValue(CommonPropKeys.cs_fisKeyspace) + "."
				+ handler.getValue(CommonPropKeys.cs_sparkAppPropCF) + " ("
				+ handler.getValue(CommonPropKeys.cs_sparkAppPropCol)
				+ ", properties) VALUES ('gs', {'host':'localhost'});");

		try {
			pHandler = new PropertiesHandler(
					"src/main/resources/local/common.properties", "gs");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void commonPropTest() {
		try {
			final String cs_entityPackagePrefix = "com.dla.foundation";
			assertEquals(cs_entityPackagePrefix,
					handler.getValue(CommonPropKeys.cs_entityPackagePrefix));
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void readFromCSTest() {
		ResultSet rs = csContext.getRows("fis", "sparkappprop", "sparkappname",
				"gs");
		String hostValue = rs.one()
				.getMap("properties", String.class, String.class).get("host");

		try {
			assertEquals(hostValue, pHandler.getValue("host"));
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void readFromFileTest() {

		String cs_entityPackagePrefix = "com.dla.foundation";
		try {
			assertEquals(cs_entityPackagePrefix,
					pHandler.getValue("cs_entityPackagePrefix"));
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void writeCSTest() {
		try {
			final String key = "ip";
			final String value = "127.0.0.1";
			pHandler.writeToCassandra(key, value);
			assertEquals(pHandler.getValue("ip"), value);
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
}
