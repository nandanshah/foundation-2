package com.dla.foundation.TestPopularity;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.ParseException;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dla.foundation.analytics.utils.CassandraContext;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.popularity.PopularityClient;
import com.dla.foundation.popularity.exception.InvalidDataException;
import com.dla.foundation.popularity.utils.ColumnCollections;
import com.dla.foundation.popularity.utils.PopularityConstants;

public class TestPopularityReco {
	private static Logger logger = Logger.getLogger(TestPopularityReco.class
			.getName());
	public CassandraContext cassandraContext;
	private static PropertiesHandler propertyHandler;

	private static String PROP_FILE_PATH;

	@Before
	public void setUp() throws Exception {
		String current_dir = System.getProperty("user.dir");
		PROP_FILE_PATH = current_dir
				+ "/../../commons/src/test/resources/common.properties";
		logger.info("Connecting to mock cassandra");
		cassandraContext = new CassandraContext(current_dir
				+ "/../../commons/src/test/resources/cassandra.yaml");
		cassandraContext.connect();
		logger.info("Connected to mock cassandra successfully");
		cassandraContext
				.executeCommand("CREATE KEYSPACE IF NOT EXISTS fistest "
						+ " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		cassandraContext
				.executeCommand("CREATE KEYSPACE IF NOT EXISTS n2test "
						+ " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");

		cassandraContext.executeCommand("use fistest");
		cassandraContext
				.executeCommand("create table if not exists eo_spark_app_prop(sparkappname text, properties map<text,text>, primary key (sparkappname))");
		String query = "insert into eo_spark_app_prop(sparkappname,properties) values('popularityReco',{'executionMode':'fullcompute','date':'2014-06-27','fullcompute_start_date':'2014-06-27','fullcompute_end_date':'2014-06-29','last_execution':'0000-00-00'}); ";
		logger.info("Executing query : " + query);
		cassandraContext.executeCommand(query);
		logger.info("Initilizing propertiesHandler with AppName "
				+ PopularityConstants.APP_NAME);
		propertyHandler = new PropertiesHandler(PROP_FILE_PATH,
				PopularityConstants.APP_NAME);
		logger.info("Initilized propertiesHandler with AppName "
				+ PopularityConstants.APP_NAME + "successfully");
		createTables();

	}

	private void createTables() {
		String createTrendDailyEventSummary = "create table if not exists trend_daily_eventsummary(periodid timeuuid ,tenantid uuid,regionid uuid,itemid uuid,eventtypeaggregate  map<text,double>,dayscore double,date timestamp,eventrequired int,primary key (periodid,tenantid,regionid,itemid));";
		String createTrendDailyDateIndex = "create index if not exists index_trend_daily_eventsummary_date on trend_daily_eventsummary (date);";
		String insertTrendDailyEventDayoneI1 = "INSERT INTO trend_daily_eventsummary(periodid,tenantid,regionid,itemid,date,dayscore,eventrequired)values(e19b0400-fd5f-11e3-8080-808080808080,c979ca35-b58d-434b-b2d6-ea0316bcc9a1,c979ca35-b58d-434b-b2d6-ea0316bcc9a1,c979ca35-b58d-434b-b2d6-ea0316bcc121, '2014-06-26 00:00:00+0530',1.5,1);";
		String insertTrendDailyEventDayoneI2 = "INSERT INTO trend_daily_eventsummary(periodid,tenantid,regionid,itemid,date,dayscore,eventrequired)values(e19b0400-fd5f-11e3-8080-808080808080,c979ca35-b58d-434b-b2d6-ea0316bcc9a1,c979ca35-b58d-434b-b2d6-ea0316bcc9a1,c979ca35-b58d-434b-b2d6-ea0316bcc122, '2014-06-26 00:00:00+0530',1.0,1);";
		String insertTrendDailyEventDaytwoI1 = "INSERT INTO trend_daily_eventsummary(periodid,tenantid,regionid,itemid,date,dayscore,eventrequired)values(0c04c400-fe29-11e3-8080-808080808080,c979ca35-b58d-434b-b2d6-ea0316bcc9a1,c979ca35-b58d-434b-b2d6-ea0316bcc9a1,c979ca35-b58d-434b-b2d6-ea0316bcc121, '2014-06-27 00:00:00+0530',1.0,1);";
		String insertTrendDailyEventDaytwoI2 = "INSERT INTO trend_daily_eventsummary(periodid,tenantid,regionid,itemid,date,dayscore,eventrequired)values(0c04c400-fe29-11e3-8080-808080808080,c979ca35-b58d-434b-b2d6-ea0316bcc9a1,c979ca35-b58d-434b-b2d6-ea0316bcc9a1,c979ca35-b58d-434b-b2d6-ea0316bcc122, '2014-06-27 00:00:00+0530',1.0,1);";
		String insertTrendDailyEventDaythreeI1 = "INSERT INTO trend_daily_eventsummary(periodid,tenantid,regionid,itemid,date,dayscore,eventrequired)values(366e8400-fef2-11e3-8080-808080808080,c979ca35-b58d-434b-b2d6-ea0316bcc9a1,c979ca35-b58d-434b-b2d6-ea0316bcc9a1,c979ca35-b58d-434b-b2d6-ea0316bcc121, '2014-06-28 00:00:00+0530',1.0,1);";
		String insertTrendDailyEventDaythreeI2 = "INSERT INTO trend_daily_eventsummary(periodid,tenantid,regionid,itemid,date,dayscore,eventrequired)values(366e8400-fef2-11e3-8080-808080808080,c979ca35-b58d-434b-b2d6-ea0316bcc9a1,c979ca35-b58d-434b-b2d6-ea0316bcc9a1,c979ca35-b58d-434b-b2d6-ea0316bcc122, '2014-06-28 00:00:00+0530',1.5,1);";
		String createPopularity = "create table if not exists popularity_reco(periodid timeuuid,tenantid uuid,regionid uuid,itemid uuid,popularityscore double,normalizedpopularityscore double,popularityscorereason text,eventrequired int,date timestamp,primary key (periodid,tenantid,regionid,itemid));";
		String createIndexPopularityDate = "CREATE INDEX if not exists popularity_reco_date ON fistest.popularity_reco (date);";
		String createIndexPopularityEvent = "CREATE INDEX if not exists popularity_reco_event_required ON fistest.popularity_reco (eventrequired);";
		cassandraContext.executeCommand(createTrendDailyEventSummary);
		logger.info("Created columnfamily trend_daily_eventsummary successfully.");
		cassandraContext.executeCommand(createPopularity);
		logger.info("Created columnfamily popularity_reco successfully.");
		cassandraContext.executeCommand(createTrendDailyDateIndex);
		logger.info("Created  index on columnfamily trend_daily_eventsummary successfully.");
		cassandraContext.executeCommand(insertTrendDailyEventDayoneI1);
		logger.info("Inserted data for day one:item one to trend_daily_eventsummary successfully.");
		cassandraContext.executeCommand(insertTrendDailyEventDayoneI2);
		logger.info("Inserted data for day one:item two to trend_daily_eventsummary successfully.");
		cassandraContext.executeCommand(insertTrendDailyEventDaytwoI1);
		logger.info("Inserted data for day two:item one to trend_daily_eventsummary successfully.");
		cassandraContext.executeCommand(insertTrendDailyEventDaytwoI2);
		logger.info("Inserted data for day two:item two to trend_daily_eventsummary successfully.");
		cassandraContext.executeCommand(insertTrendDailyEventDaythreeI1);
		logger.info("Inserted data for day three:item one to trend_daily_eventsummary successfully.");
		cassandraContext.executeCommand(insertTrendDailyEventDaythreeI2);
		logger.info("Inserted data for day three:item two to trend_daily_eventsummary successfully.");
		cassandraContext.executeCommand(createIndexPopularityDate);
		cassandraContext.executeCommand(createIndexPopularityEvent);

	}

	@Test
	public void testInit() throws IOException, ParseException,
			InvalidDataException {
		PopularityClient popularityClient = new PopularityClient();
		popularityClient.init(propertyHandler);
		assertTrue(getDataForPopularity());
	}

	private boolean getDataForPopularity() {
		ResultSet recommendationsResult = cassandraContext.getRows("fistest",
				PopularityConstants.POPULARITY_CF);
		double itemOneNormalizedScore = 0;
		double itemTwoNormalizedScore = 0;
		double itemOnePopularity = 0;
		double itemTwoPopularity = 0;
		for (Row record : recommendationsResult.all()) {
			String itemID = record.getUUID(ColumnCollections.ITEM_ID)
					.toString();
			if (record.getDate(ColumnCollections.LAST_MODIFIED).toString()
					.contains("Sun Jun 29")) {
				double normalizedScore = record
						.getDouble(ColumnCollections.NORMALIZED_SCORE);
				double popularityScore = record
						.getDouble(ColumnCollections.POPULARITY_SCORE);
				if (itemID
						.equalsIgnoreCase("c979ca35-b58d-434b-b2d6-ea0316bcc121")) {
					itemOneNormalizedScore = normalizedScore;
					itemOnePopularity = popularityScore;
				} else {
					itemTwoNormalizedScore = normalizedScore;
					itemTwoPopularity = popularityScore;
				}
			}
		}
		if (itemOneNormalizedScore == 1.0 && itemOnePopularity == 3.5) {
			if (itemTwoNormalizedScore == 1.0 && itemTwoPopularity == 3.5) {
				return true;
			}
		}
		return false;

	}

}
