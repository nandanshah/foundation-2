package com.dla.foundation.popularity;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.popularity.exception.InvalidDataException;
import com.dla.foundation.popularity.utils.CassandraConfig;
import com.dla.foundation.popularity.utils.ColumnCollections;
import com.dla.foundation.popularity.utils.PopularityConstants;

public class PopularityClient implements Serializable {

	private static final long serialVersionUID = -5996936171757717047L;
	public static String executionMode;
	public static String endDate;
	public static String startDate;
	public static String last_execution;
	private static final String CONFIG_DELIM = ",";
	private static Logger logger = Logger.getLogger(PopularityClient.class
			.getName());

	public void init(PropertiesHandler phandler) throws IOException,
			ParseException, InvalidDataException {
		String sparkHost = phandler.getValue(CommonPropKeys.spark_host
				.getValue());
		logger.info("Initialized Popularity-Reco at "
				+ new Date(System.currentTimeMillis()));
		last_execution = phandler.getValue(PopularityConstants.LAST_EXECUTION);
		CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
				getCassnadraIPArray(phandler.getValue(CommonPropKeys.cs_hostList
						.getValue())), PopularityConstants.INPUT_PARTITIONER,
				phandler.getValue(CommonPropKeys.cs_rpcPort.getValue()),
				getCassnadraIPArray(phandler
						.getValue(CommonPropKeys.cs_hostList.getValue())),
				PopularityConstants.OUTPUT_PARTITIONER);
		CassandraConfig cassandraConfig = new CassandraConfig(
				phandler.getValue(CommonPropKeys.cs_platformKeyspace.getValue()),
				phandler.getValue(CommonPropKeys.cs_fisKeyspace.getValue()),
				phandler.getValue(CommonPropKeys.cs_pageRowSize.getValue()));

		executionMode = phandler.getValue(ColumnCollections.EXECUTION_MODE);

		if (executionMode.equalsIgnoreCase(PopularityConstants.INCREMENTAL)) {
			endDate = phandler.getValue(PopularityConstants.INPUT_DATE);
		} else {
			startDate = phandler.getValue(PopularityConstants.INPUT_START_DATE);
			endDate = phandler.getValue(PopularityConstants.INPUT_END_DATE);
		}
		runPopularityCalc(sparkHost, cassandraSparkConnector, cassandraConfig,
				phandler);

		phandler.writeToCassandra(PopularityConstants.LAST_EXECUTION, endDate);
		System.out.println("Mode :"+executionMode);
	}

	public static Date getDate(String date, String dateFormat)
			throws ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		return simpleDateFormat.parse(date);
	}

	private JavaSparkContext initSparkContext(String sparkHost) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster(sparkHost);
		sparkConf.setAppName(PopularityConstants.APP_NAME);
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		return sparkContext;
	}

	public void runPopularityCalc(String sparkHost,
			CassandraSparkConnector cassandraSparkConnector,
			CassandraConfig cassandraConfig, PropertiesHandler phandler)
			throws ParseException, IOException, InvalidDataException {
		JavaSparkContext javaSparkContext = initSparkContext(sparkHost);
		decideExecutionMode();
		PopularityCalcDriver popularityCalcDriver = new PopularityCalcDriver();
		if (executionMode.equalsIgnoreCase(PopularityConstants.INCREMENTAL)) {
			popularityCalcDriver.popularityCalc(javaSparkContext,
					cassandraSparkConnector, cassandraConfig,
					getDate(endDate, PopularityConstants.DATE_FORMAT));
		} else {
			String currentDate = startDate;
			logger.info("Starting execution in "
					+ PopularityConstants.FULLCOMPUTE
					+ " mode. Start Date is : " + startDate
					+ ". End Date is : " + endDate);
			while (getDate(endDate, PopularityConstants.DATE_FORMAT).getTime() >= getDate(
					currentDate, PopularityConstants.DATE_FORMAT).getTime()) {
				logger.info("Started execution for " + currentDate);
				popularityCalcDriver.popularityCalc(javaSparkContext,
						cassandraSparkConnector, cassandraConfig,
						getDate(currentDate, PopularityConstants.DATE_FORMAT));

				phandler.writeToCassandra(PopularityConstants.LAST_EXECUTION,
						currentDate);
				logger.info("Completed execution for " + currentDate);
				currentDate = addDaysToDate(currentDate,
						PopularityConstants.DATE_FORMAT, 1);
			}
		}
		javaSparkContext.stop();

	}

	private String addDaysToDate(String currentDate, String dateFormat,
			int noOfDays) throws ParseException {
		SimpleDateFormat simpleDate = new SimpleDateFormat(dateFormat);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(simpleDate.parse(currentDate));
		calendar.add(Calendar.DATE, noOfDays); // number of days to add
		String newDate = simpleDate.format(calendar.getTime());
		return newDate;
	}

	private boolean decideExecutionMode() throws ParseException {
		if (executionMode.equalsIgnoreCase(PopularityConstants.INCREMENTAL)) {
			Date executionDate = getDate(endDate,
					PopularityConstants.DATE_FORMAT);
			// Date afterProcessedDate = DateUtils
			// .addDays(
			// getDate(last_execution,
			// PopularityConstants.DATE_FORMAT), 1);
			Date afterProcessedDateInc = DateUtils.addDays(getDate(last_execution,PopularityConstants.DATE_FORMAT),1);

			if (executionDate == getDate(last_execution,PopularityConstants.DATE_FORMAT)) {
				logger.info("Starting execution in "
						+ PopularityConstants.INCREMENTAL + " mode for "
						+ executionDate);
				return true;
			}else if (executionDate ==afterProcessedDateInc ){
				logger.info("Starting execution in "
						+ PopularityConstants.INCREMENTAL + " mode for "
						+ executionDate);
				return true;
				
			}else if (executionDate.getTime() > afterProcessedDateInc.getTime()) {
				for (int i = 0; i < 10; i++) {
					logger.warn("User provided execution mode as '"
							+ PopularityConstants.INCREMENTAL
							+ "'. But due to difference between "
							+ PopularityConstants.LAST_EXECUTION + " and "
							+ PopularityConstants.INPUT_DATE
							+ " Popularity reco is changing its mode to '"
							+ PopularityConstants.FULLCOMPUTE + "'");
				}
				startDate = last_execution;
				executionMode = PopularityConstants.FULLCOMPUTE;
			}
		}
		return false;
	}

	private String[] getCassnadraIPArray(String strCassnadraIP) {
		return strCassnadraIP.split(CONFIG_DELIM);
	}

	public static long getFormattedDate(long time) {
		Calendar date = new GregorianCalendar();
		date.setTimeInMillis(time);
		date.set(Calendar.HOUR_OF_DAY, 0);
		date.set(Calendar.MINUTE, 0);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MILLISECOND, 0);
		return date.getTimeInMillis();
	}

	public static void main(String[] args) throws IOException, ParseException,
			InvalidDataException {
		PopularityClient popularityClient = new PopularityClient();
		PropertiesHandler phanHandler = new PropertiesHandler(
				"/home/dlauser/DLA/mayur/foundation-intelligence-system/spark-apps/commons/src/test/resources/common.properties",
				PopularityConstants.APP_NAME);
		//
		popularityClient.init(phanHandler);
//		 insert into eo_spark_app_prop(sparkappname,properties)
//		 values('popularityReco',{'executionMode':'incremental','date':'2014-06-27','fullcompute_start_date':'2014-06-27','fullcompute_end_date':'2014-06-30','last_execution':'0000-00-00'});

	}
}
