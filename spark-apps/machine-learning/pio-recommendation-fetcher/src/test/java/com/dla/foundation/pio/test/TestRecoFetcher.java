package com.dla.foundation.pio.test;

import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.pio.RecommendationFetcher;
import com.dla.foundation.pio.util.ColumnCollection;
import com.dla.foundation.pio.util.PropKeys;
import com.dla.foundation.pio.util.RecoFetcherConstants;

public class TestRecoFetcher {
	private static Logger logger = Logger.getLogger(TestRecoFetcher.class
			.getName());
	private static PropertiesHandler propertyHandler;
	public static final String DEFAULT_API_PORT_NUM = "8000";
	final String TENANT_ID = "4c6f0f00-66de-1347-a621-8bfba5f4eaff";
	public CassandraContext cassandraContext;

	private static final String DEFAULT_PROFILE_FILE_PATH = "src/test/resources/profileTable.csv";
	private static final String DEFAULT_ACCOUNT_FILE_PATH = "src/test/resources/accountTable.csv";
	private static final String RESULT_FILE_PATH = "src/test/resources/result.csv";
	private static final String PROP_FILE_PATH = "D:/FoundationIntelligenceNew/foundation-intelligence-system/spark-apps/commons/src/main/resources/local/common.properties";
	private static final String INSERT_PROP = "VALUES  ('pioRecoFetcher', {'4c6f0f00-66de-1347-a621-8bfba5f4eaff':'wBYDEbrxeND3IVo4SaK3xQjRXiPJeojhZVMISSGgtbvKTiavBD4Z8qlyXkQRevtP,ItemRec','numRecsPerUser':'2','recommendCF':'pio1'});";
	private static Map<String, List<String>> mapReco;
	private static HashMap<String, String> mapRegion;

	@Before
	public void setUp() throws Exception {

		String current_dir = System.getProperty("user.dir");
		System.out.println(current_dir);

		populateRecoAndRegions();
		logger.info("Connecting to mock cassandra");
		cassandraContext = new CassandraContext();
		cassandraContext.connect();
		logger.info("Connected to mock cassandra successfully");

		cassandraContext
				.executeCommand("CREATE KEYSPACE IF NOT EXISTS fis "
						+ " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");

		cassandraContext.executeCommand("use fis");
		cassandraContext
				.executeCommand("CREATE TABLE IF NOT EXISTS fis.sparkappprop (sparkappname text, properties map<text,text>, primary key (sparkappname)); ");
		insertProp();
		logger.info("Initilizing propertiesHandler with AppName "
				+ RecoFetcherConstants.APPNAME);
		propertyHandler = new PropertiesHandler(PROP_FILE_PATH,
				RecoFetcherConstants.APPNAME);
		logger.info("Initilized propertiesHandler with AppName "
				+ RecoFetcherConstants.APPNAME + "successfully");

		cassandraContext
				.executeCommand("CREATE TABLE IF NOT EXISTS "
						+ propertyHandler
								.getValue(CommonPropKeys.cs_fisKeyspace
										.getValue())
						+ "."
						+ propertyHandler.getValue(PropKeys.PIO_RECOMMEND_CF
								.getValue())
						+ " ( tenantid uuid, regionid uuid, profileid uuid, itemid uuid ,lastrecofetched timestamp, recobyfoundationscore double,recobyfoundationreason text, eventrequired int ,primary key(profileid,itemid));");
		logger.info("Created columnfamily : "
				+ propertyHandler.getValue(PropKeys.PIO_RECOMMEND_CF.getValue()));

		cassandraContext
				.executeCommand("CREATE KEYSPACE IF NOT EXISTS "
						+ propertyHandler
								.getValue(CommonPropKeys.cs_analyticsKeyspace
										.getValue())
						+ " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		logger.info("Created keyspace : "
				+ propertyHandler.getValue(CommonPropKeys.cs_analyticsKeyspace
						.getValue()));

		cassandraContext.executeCommand("use "
				+ propertyHandler.getValue(CommonPropKeys.cs_analyticsKeyspace
						.getValue()));

		cassandraContext
				.executeCommand("CREATE TABLE IF NOT EXISTS "
						+ propertyHandler
								.getValue(CommonPropKeys.cs_analyticsKeyspace
										.getValue())
						+ "."
						+ propertyHandler.getValue(CommonPropKeys.cs_profileCF
								.getValue())
						+ " (accountid uuid,id uuid,homeregionid uuid,preferredlocaleid uuid,avatarimageurl text,birthdate timestamp,defaultcolor text,emailaddress text,emailaddress_lower text,firstname text,gender text,isadmin boolean,isrootprofile boolean,lastname text,notfiyfriendregistration boolean,notifyfriendlike boolean,notifynewrelease boolean,notifypromotional boolean,notifywatchlistexpiration boolean,passwordhash text,passwordsalt text,pinhash text,pinsalt text,profiletype text,socialauthtoken text,watchlist_id uuid,PRIMARY KEY (accountid, id));;");

		logger.info("Created columnfamily : "
				+ propertyHandler.getValue(CommonPropKeys.cs_profileCF
						.getValue()));
		cassandraContext
				.executeCommand("CREATE TABLE IF NOT EXISTS "
						+ propertyHandler
								.getValue(CommonPropKeys.cs_analyticsKeyspace
										.getValue())
						+ "."
						+ propertyHandler.getValue(CommonPropKeys.cs_accountCF
								.getValue())
						+ " (id uuid,tenantid uuid,type text,PRIMARY KEY (id));");
		logger.info("Created columnfamily : "
				+ propertyHandler.getValue(CommonPropKeys.cs_accountCF
						.getValue()));
		insertToAccountCF();
		insertToProfileCF();

	}

	private void insertProp() throws IOException {
		// String query = "INSERT INTO "
		// + propertyHandler.getValue(CommonPropKeys.cs_fisKeyspace
		// .getValue())
		// + "."
		// + propertyHandler.getValue(CommonPropKeys.cs_sparkAppPropCF
		// .getValue())
		// + " ( "
		// + propertyHandler.getValue(CommonPropKeys.cs_sparkAppPropCol
		// .getValue()) + ",properties)" + INSERT_PROP;
		// logger.info("Executing query : " + query);
		String query = "INSERT INTO fis.sparkappprop (sparkappname, properties ) "
				+ INSERT_PROP;
		logger.info("Executing query : " + query);

		cassandraContext.executeCommand(query);

	}

	private void populateRecoAndRegions() throws IOException {
		mapReco = new HashMap<String, List<String>>();
		mapRegion = new HashMap<String, String>();
		CSVFileHandler fhandler = new CSVFileHandler(RESULT_FILE_PATH, ",");
		String[] elements = null;
		String userID;
		String itemID;
		String regionID;
		while ((elements = fhandler.nextLine()) != null) {
			userID = elements[0];
			itemID = elements[1];
			regionID = elements[2];
			if (mapReco.containsKey(userID)) {
				List<String> listReco = mapReco.get(userID);
				listReco.add(itemID);
				mapReco.put(userID, listReco);
			} else {
				List<String> listReco = new ArrayList<String>();
				listReco.add(itemID);
				mapReco.put(userID, listReco);
				mapRegion.put(userID, regionID);

			}

		}

	}

	@Test
	public void test() {
		try {

			RecommendationFetcher recoFetcher = new RecommendationFetcher();
			RecommendationFetcher.TENANT_ID = TENANT_ID;
			recoFetcher.runRecommendationFetcher(propertyHandler);
			ResultSet recommendationsResult = cassandraContext.getRows(
					propertyHandler.getValue(CommonPropKeys.cs_fisKeyspace
							.getValue()), propertyHandler
							.getValue(PropKeys.PIO_RECOMMEND_CF.getValue()));

			assertTrue(verifyResult(recommendationsResult));
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	private void insertToAccountCF() throws IOException {
		CSVFileHandler fhandler = new CSVFileHandler(DEFAULT_ACCOUNT_FILE_PATH,
				",");
		String[] elements = null;
		String query = "";
		while ((elements = fhandler.nextLine()) != null) {
			String accID = elements[0];
			String tenantID = elements[1];
			query = "INSERT INTO fis.accounttable (id,tenantid) VALUES (" + accID
					+ "," + tenantID + ");";
			logger.info("Executing query : " + query);
			cassandraContext.executeCommand(query);
		}

	}

	private void insertToProfileCF() throws IOException {
		CSVFileHandler fhandler = new CSVFileHandler(DEFAULT_PROFILE_FILE_PATH,
				",");
		String[] elements = null;

		String query = "";
		while ((elements = fhandler.nextLine()) != null) {
			String accID = elements[0];
			String proID = elements[1];
			String regionID = elements[2];
			query = "INSERT INTO profiletable(accountid,id,homeregionid) VALUES ("
					+ accID + "," + proID + "," + regionID + " );";
			logger.info("Executing query : " + query);
			cassandraContext.executeCommand(query);
		}
	}

	private boolean verifyResult(ResultSet recommendationsResult) {
		//logger.info("****************  Size " +recommendationsResult.all().size());
		boolean flag = false;
		for (Row userRecord : recommendationsResult.all()) {
			flag=true;
			String uid = userRecord.getUUID(ColumnCollection.PROFILE_ID)
					.toString();
			String itemid = userRecord.getUUID(ColumnCollection.ITEM_ID)
					.toString();
			String regionid = userRecord.getUUID(ColumnCollection.REGION_ID)
					.toString();
			String tenantid = userRecord.getUUID(ColumnCollection.TENANT_ID)
					.toString();
			if (mapReco.containsKey(uid)) {
				
				List<String> listReco = mapReco.get(uid);
				if (!listReco.contains(itemid))
					return false;
				if (!mapRegion.get(uid).equalsIgnoreCase(regionid))
					return false;
				if (!tenantid.equalsIgnoreCase(TENANT_ID))
					return false;
			} else {
				return false;
			}
		}
		return flag;
	}
}
