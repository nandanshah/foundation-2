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
import com.dla.foundation.trendReco.model.DailyEventSummaryPerUserItem;
import com.dla.foundation.trendReco.model.EventType;
import com.dla.foundation.trendReco.model.UserEvent;
import com.dla.foundation.trendReco.model.UserSummary;
import com.dla.foundation.trendReco.services.UserEvtSummaryService;
import com.dla.foundation.trendReco.util.Filter;
import com.dla.foundation.trendReco.util.PropKeys;
import com.dla.foundation.trendReco.util.TrendRecoPostprocessing;
import com.dla.foundation.trendReco.util.TrendRecoProp;
import com.dla.foundation.trendReco.util.TrendRecommendationUtil;
import com.dla.foundation.trendReco.util.UserEventTransformation;

/**
 * This class provide a functionality of calculating user event summary per
 * item.
 * 
 * @author shishir_shivhare
 * @version 1.0
 * @since 2014-06-16
 * 
 * 
 */
public class UserEventSummaryDriver implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1374737648053471298L;

	private static final String TRUE = "true";
	private static final String FALSE = "false";
	private static final String DATE_FORMAT = "yyyy-MM-dd";
	private static final String IP_SEPARATOR = ",";

	private static final Logger logger = Logger
			.getLogger(UserEventSummaryDriver.class);

	/**
	 * This method will initialize the spark context, user summary service and
	 * executes them accordingly.
	 * 
	 * @param appPropFilePath
	 *            : Path of application properties having properties
	 *            app_name,read_host_list
	 *            ,write_host_list,input_partitioner,output_partitioner
	 *            ,rpc_port.
	 * 
	 * @param userSumPropFilePath
	 *            : Path of property file required by user summary calculator
	 * 
	 */
	public void runUserEvtSummaryDriver(String commPropFilePath) {

		try {
			logger.info("Initializing property handler ");

			PropertiesHandler userSumProp = new PropertiesHandler(
					commPropFilePath, TrendRecoProp.USER_EVENT_SUM_APP_NAME);

			// initializing spark context
			logger.info("initializing spark context");
			JavaSparkContext sparkContext = new JavaSparkContext(
					userSumProp.getValue(CommonPropKeys.spark_host.getValue()),
					TrendRecoProp.USER_EVENT_SUM_APP_NAME);

			// initializing spark-cassandra connector
			logger.info("initializing spark-cassandra connector");
			CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
					TrendRecommendationUtil
							.getList(CommonPropKeys.cs_hostList.getValue(),
									IP_SEPARATOR),
					TrendRecoProp.PARTITIONER,
					userSumProp.getValue(CommonPropKeys.cs_rpcPort.getValue()),
					TrendRecommendationUtil.getList(userSumProp
							.getValue(CommonPropKeys.cs_hostList.getValue()),
							","), TrendRecoProp.PARTITIONER);
			runUserEvtSummaryDriver(sparkContext, cassandraSparkConnector,
					userSumProp);

		} catch (Exception e) {
			logger.error(e);
		}

	}

	public void runUserEvtSummaryDriver(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			PropertiesHandler userSumProp) throws Exception {
		try {

			logger.info("Initializing query for user Summary");
			final String USER_SUMM_QUERY_PROPERTY = "UPDATE "
					+ userSumProp.getValue(CommonPropKeys.cs_fisKeyspace
							.getValue()) + "."
					+ TrendRecoProp.USER_EVENT_SUM_OUT_CF + " SET "
					+ DailyEventSummaryPerUserItem.EVENT_AGGREGATE.getColumn()
					+ "=?,"
					+ DailyEventSummaryPerUserItem.DAY_SCORE.getColumn()
					+ "=?," + DailyEventSummaryPerUserItem.DATE.getColumn()
					+ "=?,"
					+ DailyEventSummaryPerUserItem.EVENTREQUIRED.getColumn()
					+ "=?";

			Map<String, EventType> requiredEventType = TrendRecommendationUtil
					.getRequiredEvent(userSumProp
							.getValue(PropKeys.EVENT_REQUIRED.getValue())); 

			logger.info("initializing  user Summary service with required events");
			// initializing user Summary service with required events
			UserEvtSummaryService userSummaryService = new UserEvtSummaryService(
					requiredEventType);

			logger.info("initializing cassandra config for  user Summary service");
			// initializing cassandra config for user Summary service
			CassandraConfig userSummCassandraProp = new CassandraConfig(
					userSumProp.getValue(CommonPropKeys.cs_fisKeyspace
							.getValue()),
					userSumProp.getValue(CommonPropKeys.cs_fisKeyspace
							.getValue()),
					TrendRecoProp.USER_EVENT_SUM_INP_CF,
					TrendRecoProp.USER_EVENT_SUM_OUT_CF,
					userSumProp.getValue(CommonPropKeys.cs_pageRowSize
							.getValue()), USER_SUMM_QUERY_PROPERTY);

			String incrementalFlag = userSumProp
					.getValue(PropKeys.INCREMENTAL_FLAG.getValue());

			if (incrementalFlag.toLowerCase().compareTo(TRUE) == 0) {
				logger.info("Executing incremental module of user summary");
				Date inputDate = TrendRecommendationUtil.getDate(
						userSumProp.getValue(PropKeys.INPUT_DATE.getValue()),
						DATE_FORMAT);
				UserEvtSummaryConfig userSummaryConfig = new UserEvtSummaryConfig(
						inputDate, inputDate, requiredEventType);
				logger.info("Executing user summary calculator");
				userEvtSummaryCalculator(sparkContext, cassandraSparkConnector,
						userSummaryService, userSummaryConfig,
						userSummCassandraProp);
				
				Date input_date_user_event = DateUtils.addDays(
						TrendRecommendationUtil.getDate(userSumProp
								.getValue(PropKeys.INPUT_DATE.getValue()),
								DATE_FORMAT), 1);

				userSumProp.writeToCassandra(PropKeys.INPUT_DATE.getValue(),
						TrendRecommendationUtil.getDate(input_date_user_event,
								DATE_FORMAT));
				
			} else if (incrementalFlag.toLowerCase().compareTo(FALSE) == 0) {
				logger.info("Executing Recalculation module of user summary");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
						DATE_FORMAT);
				Date startDate = TrendRecommendationUtil.getDate(userSumProp
						.getValue(PropKeys.RECAL_START_DATE.getValue()),
						DATE_FORMAT);
				Date endDate = TrendRecommendationUtil.getDate(userSumProp
						.getValue(PropKeys.RECAL_END_DATE.getValue(),
								simpleDateFormat.format(DateUtils.addDays(
										new Date(), -1))), DATE_FORMAT);
				UserEvtSummaryConfig userSummaryConfig = new UserEvtSummaryConfig(
						startDate, endDate, requiredEventType,
						Integer.parseInt(userSumProp
								.getValue(PropKeys.RECAL_PERIOD.getValue())));
				logger.info("Executing user summary Recalculator");
				userEvtSummaryRecalculator(sparkContext,
						cassandraSparkConnector, userSummaryService,
						userSummaryConfig, userSummCassandraProp);

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

		}

	}

	/**
	 * 
	 * @param sparkContext
	 *            : spark context initialized with mode and name.
	 * @param cassandraSparkConnector
	 *            : cassandra context initialized with input and output ips and
	 *            partitioner.
	 * @param userSummaryService
	 *            : Use Summary service provides algorithm to calculate user
	 *            summary per item.
	 * @param userSummaryConfig
	 *            : Configuration required by user summary.
	 * @param userSummaryCassandraProp
	 *            : cassandra property required by spark to read and write data
	 *            from/to cassandra (input keyspace,output keyspace,input column
	 *            family,output columnfamily,page row size)
	 */
	public void userEvtSummaryCalculator(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			UserEvtSummaryService userSummaryService,
			UserEvtSummaryConfig userSummaryConfig,
			CassandraConfig userSummaryCassandraProp) {
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD;
		logger.info("Reading records from user event columnfamily");
		Configuration conf = new Configuration();
		if (null == userSummaryConfig.endDate
				|| userSummaryConfig.startDate
						.equals(userSummaryConfig.endDate)) {
			// It has where condition which allows it to fetch data only for the
			// provided date.
			cassandraRDD = cassandraSparkConnector.read(conf, sparkContext,
					userSummaryCassandraProp.getInputKeyspace(),
					userSummaryCassandraProp.getInputColumnfamily(),
					userSummaryCassandraProp.getPageRowSize(),
					TrendRecommendationUtil
							.getWhereClause(userSummaryConfig.startDate));
		} else {
			cassandraRDD = cassandraSparkConnector.read(conf, sparkContext,
					userSummaryCassandraProp.getInputKeyspace(),
					userSummaryCassandraProp.getInputColumnfamily(),
					userSummaryCassandraProp.getPageRowSize(),
					TrendRecommendationUtil.getWhereClause(
							userSummaryConfig.startDate,
							userSummaryConfig.endDate));

		}
		logger.info("Executing user summary service");
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = userEvtSummaryService(
				cassandraRDD, userSummaryService, userSummaryConfig);

		logger.info("Writing records to daily event summary per item columnfamily");
		cassandraSparkConnector.write(conf,
				userSummaryCassandraProp.getOutputKeyspace(),
				userSummaryCassandraProp.getOutputColumnfamily(),
				userSummaryCassandraProp.getOutputQuery(), cassandraOutputRDD);

	}

	/**
	 * This function will provide the functionality to recalculate the user
	 * summary. It executes when incremental flag is false. It recalculate the
	 * user summary for the specified start and end date. If end date is not
	 * specified then it will take 1 day before present date i.e today's date
	 * -1.
	 * 
	 * @param sparkContext
	 * @param cassandraService
	 * @param userSummaryService
	 * @param userSummaryConfig
	 * @param userSummCassandraProp
	 * @throws Exception
	 */
	public void userEvtSummaryRecalculator(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraService,
			UserEvtSummaryService userSummaryService,
			UserEvtSummaryConfig userSummaryConfig,
			CassandraConfig userSummCassandraProp) throws Exception {

		Date startDate = userSummaryConfig.startDate;
		Date endDate = userSummaryConfig.endDate;
		int recalPeriod = userSummaryConfig.periodForRecal;
		if (null == startDate || null == endDate) {
			throw new Exception(
					"Please provide the start and end date for recalculation");
		}
		if (startDate.equals(endDate)) {
			userSummaryConfig.startDate = startDate;
			userEvtSummaryCalculator(sparkContext, cassandraService,
					userSummaryService, userSummaryConfig,
					userSummCassandraProp);
		} else if (startDate.before(endDate)) {
			if (recalPeriod <= 0) {
				throw new Exception(
						"recalculation period should not be 0 and -ve");
			}
			List<Date> dates = TrendRecommendationUtil.getAllDates(startDate,
					endDate, recalPeriod);

			for (int i = 0; i < dates.size(); i++) {
				if (i + 1 == dates.size()) {
					userSummaryConfig.endDate = endDate;
					userEvtSummaryCalculator(sparkContext, cassandraService,
							userSummaryService, userSummaryConfig,
							userSummCassandraProp);
					break;
				}
				userSummaryConfig.startDate = dates.get(i);
				userSummaryConfig.endDate = dates.get(i + 1);
				userEvtSummaryCalculator(sparkContext, cassandraService,
						userSummaryService, userSummaryConfig,
						userSummCassandraProp);
			}

		} else {
			logger.error("Start date provided is a future date");
		}

	}

	/**
	 * This service is used to convert the record in the format in which user
	 * summary service requires as well as cassandra requires while reading or
	 * writing data from or to column family.
	 */
	private JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> userEvtSummaryService(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD,
			UserEvtSummaryService userSummaryService,
			UserEvtSummaryConfig userSummaryConfig) {
		logger.info("Preprocessing before executing user summary service");
		JavaPairRDD<String, UserEvent> userEventRDD = preprocessingForUserEvtSummary(
				cassandraRDD, userSummaryConfig.requiredEvent);

		logger.info("Executing user summary algorithm");
		JavaRDD<UserSummary> dayScoreEventRDD = userSummaryService
				.calculateUserSummary(userEventRDD);

		logger.info("Postprocessing after executing  user summary service - preparing record in the format in which spark-cassandra connector require.");
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = TrendRecoPostprocessing
				.processingForUserSummary(dayScoreEventRDD);
		return cassandraOutputRDD;

	}

	/**
	 * @param cassandraRDD
	 * @param requiredEventMap
	 *            : Map containing events which are required with its weight and
	 *            threshold while calculating user summary and day score per
	 *            user basis.
	 * @return
	 */
	private JavaPairRDD<String, UserEvent> preprocessingForUserEvtSummary(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD,
			Map<String, EventType> requiredEventMap) {
		logger.info("Preprocessing: transforming record from cassandra format to required user event ");
		JavaPairRDD<String, UserEvent> userEventRDD = UserEventTransformation
				.getUserEventWithKey(cassandraRDD);
		logger.info("Preprocessing: Filtering incomplete records");
		JavaPairRDD<String, UserEvent> filteredUserEventRDD = Filter
				.filterUserEvent(userEventRDD);
		return filteredUserEventRDD;

	}

	public class UserEvtSummaryConfig {
		public Date startDate;
		public Date endDate;
		public int periodForRecal;
		public Map<String, EventType> requiredEvent;

		public UserEvtSummaryConfig(Date startDate, Date endDate,
				Map<String, EventType> requiredEvent, int periodForRecal) {
			super();
			this.startDate = startDate;
			this.endDate = endDate;
			this.requiredEvent = requiredEvent;
			this.periodForRecal = periodForRecal;
		}

		public UserEvtSummaryConfig(Date startDate, Date endDate,
				Map<String, EventType> requiredEvent) {
			this(startDate, endDate, requiredEvent, -1);
		}
	}

	public static void main(String[] args) {
		UserEventSummaryDriver trendRecoSer = new UserEventSummaryDriver();
		if (args.length == 1) {
			trendRecoSer.runUserEvtSummaryDriver(args[0]);
		} else {
			System.err.println("Please provide the path of property file");
		}

	}

}
