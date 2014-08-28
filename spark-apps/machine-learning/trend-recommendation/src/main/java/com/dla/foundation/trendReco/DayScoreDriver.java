package com.dla.foundation.trendReco;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.trendReco.model.CassandraConfig;
import com.dla.foundation.trendReco.model.DailyEventSummaryPerItem;
import com.dla.foundation.trendReco.model.DayScore;
import com.dla.foundation.trendReco.model.UserSummary;
import com.dla.foundation.trendReco.services.DayScoreService;
import com.dla.foundation.trendReco.util.Filter;
import com.dla.foundation.trendReco.util.PropKeys;
import com.dla.foundation.trendReco.util.TrendRecoPostprocessing;
import com.dla.foundation.trendReco.util.TrendRecoProp;
import com.dla.foundation.trendReco.util.TrendRecommendationUtil;
import com.dla.foundation.trendReco.util.UserSummaryTransformation;

/**
 * This class provide a functionality of aggregating day score per item basis.
 * 
 * @author shishir_shivhare
 * @version 1.0
 * @since 2014-06-16
 * 
 * 
 */
public class DayScoreDriver implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1374737648053471298L;

	private static final String TRUE = "true";
	private static final String FALSE = "false";
	private static final String DATE_FORMAT = "yyyy-MM-dd";
	private static final Logger logger = Logger.getLogger(DayScoreDriver.class);

	/**
	 * This method will initialize the spark context, daily score service and
	 * executes them accordingly.
	 * 
	 * @param appPropFilePath
	 *            : Path of application properties having properties
	 *            app_name,read_host_list
	 *            ,write_host_list,input_partitioner,output_partitioner
	 *            ,rpc_port.
	 * 
	 * @param DailyEventSumPropFilePath
	 *            : Path of property file required by day score calculator
	 * 
	 */
	public void runDayScoreDriver(String commPropFilePath) {
		try {
			logger.info("Initializing property handler ");

			PropertiesHandler dayScoreProp = new PropertiesHandler(
					commPropFilePath,
					TrendRecoProp.DAILY_EVENT_SUMMARY_APP_NAME);

			// initializing spark context
			logger.info("initializing spark context");
			JavaSparkContext sparkContext = new JavaSparkContext(
					dayScoreProp.getValue(CommonPropKeys.spark_host.getValue()),
					TrendRecoProp.DAILY_EVENT_SUMMARY_APP_NAME);

			// initializing cassandra service
			logger.info("initializing spark-cassandra connector");
			CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
					TrendRecommendationUtil.getList(dayScoreProp
							.getValue(CommonPropKeys.cs_hostList.getValue()),
							","),
					TrendRecoProp.PARTITIONER,
					dayScoreProp.getValue(CommonPropKeys.cs_rpcPort.getValue()),
					TrendRecommendationUtil.getList(dayScoreProp
							.getValue(CommonPropKeys.cs_hostList.getValue()),
							","), TrendRecoProp.PARTITIONER);

			runDayScoreDriver(sparkContext, cassandraSparkConnector,
					dayScoreProp);

		} catch (Exception e) {
			logger.error(e);
		}

	}

	public void runDayScoreDriver(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			PropertiesHandler dailyEventSumProp) throws Exception {
		try {

			logger.info("Initializing query for day score");
			// initializing query for day score
			final String DAY_SCORE_QUERY_PROPERTY = "UPDATE "
					+ dailyEventSumProp.getValue(CommonPropKeys.cs_fisKeyspace
							.getValue()) + "." + TrendRecoProp.DAY_SCORE_OUT_CF
					+ " SET "
					+ DailyEventSummaryPerItem.EVENT_AGGREGATE.getColumn()
					+ " =?," + DailyEventSummaryPerItem.DAY_SCORE.getColumn()
					+ "=?," + DailyEventSummaryPerItem.DATE.getColumn() + "=?,"
					+ DailyEventSummaryPerItem.EVENTREQUIERD.getColumn() + "=?";

			logger.info("initializing dayscore service");
			// initializing dayscore service
			DayScoreService dayScoreService = new DayScoreService();

			logger.info("initializing cassandra config for day score service");
			// initializing cassandra config for day score service
			CassandraConfig dataScoreCassandraProp = new CassandraConfig(
					dailyEventSumProp.getValue(CommonPropKeys.cs_fisKeyspace
							.getValue()),
					dailyEventSumProp.getValue(CommonPropKeys.cs_fisKeyspace
							.getValue()), TrendRecoProp.DAY_SCORE_INP_CF,
					TrendRecoProp.DAY_SCORE_OUT_CF,
					dailyEventSumProp.getValue(CommonPropKeys.cs_pageRowSize
							.getValue()), DAY_SCORE_QUERY_PROPERTY);

			String incrementalFlag = dailyEventSumProp
					.getValue(PropKeys.INCREMENTAL_FLAG.getValue());

			if (incrementalFlag.toLowerCase().compareTo(TRUE) == 0) {
				logger.info("Executing incremenatl module");
				Date inputDate = TrendRecommendationUtil.getDate(
						dailyEventSumProp.getValue(PropKeys.INPUT_DATE
								.getValue()), DATE_FORMAT);
				DayScoreConfig dayScoreConfig = new DayScoreConfig(inputDate);
				logger.info("Executing day score calculator");
				dayScoreCalculator(sparkContext, cassandraSparkConnector,
						dayScoreService, dayScoreConfig, dataScoreCassandraProp);
				
				Date input_date_daily_event = DateUtils.addDays(
						TrendRecommendationUtil.getDate(dailyEventSumProp
								.getValue(PropKeys.INPUT_DATE.getValue()),
								DATE_FORMAT), 1);

				dailyEventSumProp.writeToCassandra(PropKeys.INPUT_DATE
						.getValue(), TrendRecommendationUtil.getDate(
						input_date_daily_event, DATE_FORMAT));
								
			} else if (incrementalFlag.toLowerCase().compareTo(FALSE) == 0) {
				logger.info("Executing recalculation module");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
						DATE_FORMAT);
				Date startDate = TrendRecommendationUtil.getDate(
						dailyEventSumProp.getValue(PropKeys.RECAL_START_DATE
								.getValue()), DATE_FORMAT);
				Date endDate = TrendRecommendationUtil.getDate(
						dailyEventSumProp.getValue(PropKeys.RECAL_END_DATE
								.getValue(), simpleDateFormat.format(DateUtils
								.addDays(new Date(), -1))), DATE_FORMAT);
				DayScoreConfig dayScoreConfig = new DayScoreConfig(startDate,
						endDate, Integer.parseInt(dailyEventSumProp
								.getValue(PropKeys.RECAL_PERIOD.getValue())));
				logger.info("Executing day score Recalculator");
				dayScoreRecalculator(sparkContext, cassandraSparkConnector,
						dayScoreService, dayScoreConfig, dataScoreCassandraProp);

			} else {
				throw new Exception(
						"Please provide input date (input_date) for incremental processing or start date (start_date) for full recalculation with incremental_flag (true will be for incremental processing of input date and false will be for full recalculation from specified start date to end date");

			}

		} catch (ParseException e) {
			logger.error("Please provide proper input date in the format "
					+ DATE_FORMAT + "\n " + e);
			throw new Exception(
					"Please provide proper input date in the format "
							+ DATE_FORMAT + "\n " + e);

		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());

		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		} finally {
			dailyEventSumProp.close();
		}
	}

	/**
	 * 
	 * @param sparkContext
	 *            : spark context initialized with mode and name.
	 * @param cassandraService
	 *            : cassandra context initialized with input and output ips and
	 *            partitioner.
	 * @param dayScoreService
	 *            : Day score service provides day score algorithm to aggregate
	 *            days score.
	 * @param inputDate
	 *            : Date for which day score has to be calculated.
	 * @param dataScoreCassandraProp
	 *            : cassandra property required by spark to read and write data
	 *            from/to cassandra (input keyspace,output keyspace,input column
	 *            family,output columnfamily,page row size)
	 */
	public void dayScoreCalculator(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			DayScoreService dayScoreService, DayScoreConfig dayScoreConfig,
			CassandraConfig dataScoreCassandraProp) {

		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD;
		Configuration conf = new Configuration();

		if (null == dayScoreConfig.endDate
				|| dayScoreConfig.startDate.equals(dayScoreConfig.endDate)) {
			logger.info("Reading records from daily event summary per user item columnfamily for date: "
					+ dayScoreConfig.startDate);
			// It has where condition which allows it to fetch data only for the
			// provided date.
			cassandraRDD = cassandraSparkConnector.read(conf, sparkContext,
					dataScoreCassandraProp.getInputKeyspace(),
					dataScoreCassandraProp.getInputColumnfamily(),
					dataScoreCassandraProp.getPageRowSize(),
					TrendRecommendationUtil
							.getWhereClause(dayScoreConfig.startDate));
		} else {
			// It has where condition which allows it to fetch data only for the
			// provided date.
			logger.info("Reading records from daily event summary per user item columnfamily for date: "
					+ dayScoreConfig.startDate + " to" + dayScoreConfig.endDate);
			cassandraRDD = cassandraSparkConnector.read(conf, sparkContext,
					dataScoreCassandraProp.getInputKeyspace(),
					dataScoreCassandraProp.getInputColumnfamily(),
					dataScoreCassandraProp.getPageRowSize(),
					TrendRecommendationUtil.getWhereClause(
							dayScoreConfig.startDate, dayScoreConfig.endDate));

		}

		logger.info("Executing Day score service");
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = dayScoreService(
				cassandraRDD, dayScoreService, dayScoreConfig);

		logger.info("Writing records to daily event summary columnfamily");
		cassandraSparkConnector.write(conf,
				dataScoreCassandraProp.getOutputKeyspace(),
				dataScoreCassandraProp.getOutputColumnfamily(),
				dataScoreCassandraProp.getOutputQuery(), cassandraOutputRDD);

	}

	public void dayScoreRecalculator(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			DayScoreService dayScoreService, DayScoreConfig dayScoreConfig,
			CassandraConfig dataScoreCassandraProp) throws Exception {

		Date startDate = dayScoreConfig.startDate;
		Date endDate = dayScoreConfig.endDate;
		int recalPeriod = dayScoreConfig.periodForRecal;
		if (null == startDate || null == endDate) {
			throw new Exception(
					"Please provide the start and end date for recalculation");
		}
		if (startDate.equals(endDate)) {
			dayScoreConfig.startDate = startDate;

			dayScoreCalculator(sparkContext, cassandraSparkConnector,
					dayScoreService, dayScoreConfig, dataScoreCassandraProp);
		} else if (startDate.before(endDate)) {
			if (recalPeriod <= 0) {
				throw new Exception(
						"recalculation period should not be 0 and -ve");
			}
			logger.info("Recalculator: Preparing list of dates for incremental module");
			List<Date> dates = TrendRecommendationUtil.getAllDates(startDate,
					endDate, recalPeriod);

			for (int i = 0; i < dates.size(); i++) {
				if (i + 1 == dates.size()) {
					dayScoreConfig.endDate = endDate;
					dayScoreCalculator(sparkContext, cassandraSparkConnector,
							dayScoreService, dayScoreConfig,
							dataScoreCassandraProp);
					break;
				}
				dayScoreConfig.startDate = dates.get(i);
				dayScoreConfig.endDate = dates.get(i + 1);
				dayScoreCalculator(sparkContext, cassandraSparkConnector,
						dayScoreService, dayScoreConfig, dataScoreCassandraProp);
			}

		} else {
			logger.error("Start date provided is a future date");
		}

	}

	/**
	 * 
	 * This service is used to convert the record in the format in which day
	 * score service requires as well as cassandra requires while
	 * reading/writing data from/to column family.
	 */
	private JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> dayScoreService(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD,
			DayScoreService dayScoreService, DayScoreConfig dayScoreConfig) {
		logger.info("Preprocessing before executing day score service");
		JavaPairRDD<String, UserSummary> userSummaryRDD = preprocessingForDayScore(cassandraRDD);

		logger.info("Executing day score algorithm");
		JavaRDD<DayScore> dayScoreEventRDD = dayScoreService
				.calculateScore(userSummaryRDD);

		logger.info("Postprocessing after executing day score service- preparing record in the format in which spark-cassandra require.");
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = TrendRecoPostprocessing
				.processingForDayScore(dayScoreEventRDD);
		return cassandraOutputRDD;

	}

	/**
	 * This function will transform the record into user summary and also
	 * removes are records which are incomplete.
	 * 
	 * @param cassandraRDD
	 * @return
	 */
	private JavaPairRDD<String, UserSummary> preprocessingForDayScore(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {
		logger.info("Preprocessing: transforming record from cassandra format to required user summary ");
		JavaPairRDD<String, UserSummary> userSummarytRDD = UserSummaryTransformation
				.getUserSummaryWithKey(cassandraRDD);
		logger.info("Preprocessing: Filtering incomplete records");
		JavaPairRDD<String, UserSummary> filteredUserEventRDD = Filter
				.filterUserSummaryEvent(userSummarytRDD);
		return filteredUserEventRDD;
	}

	public class DayScoreConfig {
		public Date startDate;
		public Date endDate;
		public int periodForRecal;

		public DayScoreConfig(Date startDate, Date endDate, int periodForRecal) {
			super();
			this.startDate = startDate;
			this.endDate = endDate;
			this.periodForRecal = periodForRecal;
		}

		public DayScoreConfig(Date inputDate) {
			this(inputDate, null, -1);
		}
	}

	public static void main(String[] args) {
		DayScoreDriver trendRecoSer = new DayScoreDriver();
		if (args.length == 1) {
			trendRecoSer.runDayScoreDriver(args[0]);
		} else {
			System.err.println("Please provide the path of property file");
		}

	}

}
