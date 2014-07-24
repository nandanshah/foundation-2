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
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.trendReco.model.CassandraConfig;
import com.dla.foundation.trendReco.model.DayScore;
import com.dla.foundation.trendReco.model.Trend;
import com.dla.foundation.trendReco.model.TrendScore;
import com.dla.foundation.trendReco.services.ITrendScore;
import com.dla.foundation.trendReco.services.ZScoreService;
import com.dla.foundation.trendReco.util.DayScoreTransformation;
import com.dla.foundation.trendReco.util.Filter;
import com.dla.foundation.trendReco.util.PropKeys;
import com.dla.foundation.trendReco.util.TrendRecoPostprocessing;
import com.dla.foundation.trendReco.util.TrendRecommendationUtil;

/**
 * This class provide a functionality of calculating zscore with normalized
 * score.
 * 
 * @author shishir_shivhare
 * @version 1.0
 * @since 2014-06-16
 * 
 * 
 */
public class TrendScoreDriver implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1374737648053471298L;

	private static final String TRUE = "true";
	private static final String FALSE = "false";
	private static final String DATE_FORMAT = "yyyy-MM-dd";

	private static final Logger logger = Logger
			.getLogger(TrendScoreDriver.class);

	/**
	 * This method will initialize the spark context,zscore service and executes
	 * them accordingly.
	 * 
	 * @param appPropFilePath
	 *            : Path of application properties having properties
	 *            app_name,read_host_list
	 *            ,write_host_list,input_partitioner,output_partitioner
	 *            ,rpc_port.
	 * 
	 * @param trendRecoPropFilePath
	 *            : Path of property file required by zscore calculator
	 * 
	 */
	public void runTrendScoreDriver(String appPropFilePath,
			String trendRecoPropFilePath) {
		try {
			logger.info("Initializing property handler ");
			// Initializing property handler
			PropertiesHandler appProp = new PropertiesHandler(appPropFilePath);
			PropertiesHandler trendScoreProp = new PropertiesHandler(
					trendRecoPropFilePath);

			// initializing query for zscore
			logger.info("initializing query for zscore");
			final String TREND_SCORE_QUERY_PROPERTY = "UPDATE "
					+ trendScoreProp.getValue(PropKeys.OUTPUT_KEYSPACE
							.getValue())
					+ "."
					+ trendScoreProp.getValue(PropKeys.OUTPUT_COLUMNFAMILY
							.getValue()) + " SET "
					+ Trend.TREND_SCORE.getColumn() + " =?, "
					+ Trend.NORMALIZED_SCORE.getColumn() + " =?"
					+ Trend.TREND_SCORE_REASON.getColumn() + " =?";

			// initializing spark context
			logger.info("initializing spark context");
			JavaSparkContext sparkContext = new JavaSparkContext(
					appProp.getValue(PropKeys.MODE_PROPERTY.getValue()),
					appProp.getValue(PropKeys.APP_NAME.getValue()));

			// initializing cassandra service
			logger.info("initializing cassandra service");
			CassandraSparkConnector cassandraSparkConnector = new CassandraSparkConnector(
					TrendRecommendationUtil
							.getList(appProp.getValue(PropKeys.INPUT_HOST_LIST
									.getValue()), ","),
					appProp.getValue(PropKeys.INPUT_PARTITIONER.getValue()),
					appProp.getValue(PropKeys.INPUT_RPC_PORT.getValue()),
					TrendRecommendationUtil.getList(appProp
							.getValue(PropKeys.OUTPUT_HOST_LIST.getValue()),
							","), appProp.getValue(PropKeys.OUTPUT_PARTITIONER
							.getValue()));

			logger.info("initializing cassandra config for zscore service");
			// initializing cassandra config for zscore service
			CassandraConfig trendScoreCassandraProp = new CassandraConfig(
					trendScoreProp.getValue(PropKeys.INPUT_KEYSPACE.getValue()),
					trendScoreProp.getValue(PropKeys.OUTPUT_KEYSPACE.getValue()),
					trendScoreProp.getValue(PropKeys.INPUT_COLUMNFAMILY
							.getValue()), trendScoreProp
							.getValue(PropKeys.OUTPUT_COLUMNFAMILY.getValue()),
					trendScoreProp.getValue(PropKeys.PAGE_ROW_SIZE.getValue()),
					TREND_SCORE_QUERY_PROPERTY);

			logger.info("initializing  zscore service with current date and period");
			// initializing zscore service with current date and period
			ZScoreService zScoreService = (ZScoreService) TrendRecommendationUtil
					.getService(
							trendScoreProp.getValue(PropKeys.FULL_QUALIFIED_CLASSNAME
									.getValue()), ITrendScore.class)
					.getConstructor(Long.class, int.class)
					.newInstance(
							TrendRecommendationUtil
									.getDate(
											trendScoreProp.getValue(PropKeys.CURRENT_TREND_DATE
													.getValue()), DATE_FORMAT)
									.getTime(),
							Integer.parseInt(trendScoreProp
									.getValue(PropKeys.ZSCORE_PERIOD.getValue())));

			String incrementalFlag = trendScoreProp
					.getValue(PropKeys.INCREMENTAL_FLAG.getValue());

			if (incrementalFlag.toLowerCase().compareTo(TRUE) == 0) {
				logger.info("Executing Incremental module");
				Date currentTrendDate = TrendRecommendationUtil.getDate(
						trendScoreProp.getValue(PropKeys.CURRENT_TREND_DATE
								.getValue()), DATE_FORMAT);
				TrendScoreConfig trendScoreConfig = new TrendScoreConfig(
						currentTrendDate, Integer.parseInt(trendScoreProp
								.getValue(PropKeys.ZSCORE_PERIOD.getValue())));
				logger.info("Executing Trend score calculator");
				trendScoreCalculator(sparkContext, cassandraSparkConnector,
						zScoreService, trendScoreCassandraProp,
						trendScoreConfig);
			} else if (incrementalFlag.toLowerCase().compareTo(FALSE) == 0) {
				logger.info("Executing Recalculation module");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
						DATE_FORMAT);
				Date startDate = TrendRecommendationUtil.getDate(trendScoreProp
						.getValue(PropKeys.RECAL_START_DATE.getValue()),
						DATE_FORMAT);
				Date endDate = TrendRecommendationUtil.getDate(trendScoreProp
						.getValue(PropKeys.RECAL_END_DATE.getValue(),
								simpleDateFormat.format(DateUtils.addDays(
										new Date(), -1))), DATE_FORMAT);
				TrendScoreConfig trendScoreConfig = new TrendScoreConfig(
						startDate, endDate, Integer.parseInt(trendScoreProp
								.getValue(PropKeys.ZSCORE_PERIOD.getValue())));
				logger.info("Executing Trend score Recalculator");
				trendScoreReCalculator(sparkContext, cassandraSparkConnector,
						zScoreService, trendScoreCassandraProp,
						trendScoreConfig);

			} else {
				throw new Exception(
						"Please provide input date (input_date) for incremental processing or start date (start_date) for full recalculation with incremental_flag (true will be for incremental processing of input date and false will be for full recalculation from specified start date to end date");

			}

		} catch (NumberFormatException e) {
			logger.error("Please provide proper zscore period " + e);
		} catch (ParseException e) {
			logger.error("Please provide proper current trend date in the format "
					+ DATE_FORMAT + "\n " + e);

		} catch (IOException e) {
			logger.error(e);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | NoSuchMethodException
				| SecurityException | ClassNotFoundException e) {
			logger.error("Exception occured while loading class dynaamically :"
					+ e);
			e.printStackTrace();
		} catch (Exception e) {
			logger.error(e);
		}
	}

	/**
	 * This method provides the functionality of reading data from
	 * cassandra,filtering records,computing trend score and writing result back
	 * to cassandra.
	 * 
	 * @param sparkContext
	 *            : spark context intialized with mode and name.
	 * @param cassandraService
	 *            : cassandra context intialized with input and output ips and
	 *            partitioner.
	 * @param zScoreService
	 *            : Zscore service provides the zscore algorithm to computes
	 *            zscore with filtering of events on the basis of date, events.
	 * @param trendScoreCassandraProp
	 *            : cassandra property required by spark to read and write data
	 *            from/to cassandra (input keyspace,output keyspace,input column
	 *            family,output columnfamily,page row size)
	 * @throws Exception
	 */
	public void trendScoreCalculator(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			ZScoreService zScoreService,
			CassandraConfig trendScoreCassandraProp,
			TrendScoreConfig trendScoreConfig) throws Exception {
		// reading data from cassandra
		logger.info("Reading records from daily summary event columnfamily");
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD;
		Configuration conf = new Configuration();
		if (null == trendScoreConfig.endDate
				|| trendScoreConfig.startDate.equals(trendScoreConfig.endDate)) {
			if (trendScoreConfig.periodForHistoryData < 0) {
				throw new Exception(
						"period for histroy data cannot be negative");
			}
			logger.info("Reading records from daily event summary columnfamily for date: "
					+ trendScoreConfig.startDate);
			// It has where condition which allows it to fetch data only for the
			// provided date.
			cassandraRDD = cassandraSparkConnector.read(conf, sparkContext,
					trendScoreCassandraProp.getInputKeyspace(),
					trendScoreCassandraProp.getInputColumnfamily(),
					trendScoreCassandraProp.getPageRowSize(),
					TrendRecommendationUtil.getWhereClause(
							trendScoreConfig.startDate,
							trendScoreConfig.periodForHistoryData));
		} else {
			throw new Exception(
					"Start & Date Cannot be Different in Trend Recommendation when incremental processing is running or make incremental processing false");
		}
		logger.info("Executing trend score calculator");
		// calling trend score
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = trendScoreService(
				cassandraRDD, zScoreService);

		logger.info("Writing records to trend columnfamily ");
		// writing data to cassandra
		cassandraSparkConnector.write(conf,
				trendScoreCassandraProp.getOutputKeyspace(),
				trendScoreCassandraProp.getOutputColumnfamily(),
				trendScoreCassandraProp.getOutputQuery(), cassandraOutputRDD);

	}

	public void trendScoreReCalculator(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			ZScoreService zScoreService,
			CassandraConfig trendScoreCassandraProp,
			TrendScoreConfig trendScoreConfig) throws Exception {
		logger.info("Preparing list for dates for trend score");
		List<Date> dates = TrendRecommendationUtil.getAllDates(
				trendScoreConfig.startDate, trendScoreConfig.endDate, 1);
		for (Date date : dates) {
			trendScoreConfig.startDate = date;
			trendScoreConfig.endDate = date;
			trendScoreCalculator(sparkContext, cassandraSparkConnector,
					zScoreService, trendScoreCassandraProp, trendScoreConfig);
		}
	}

	/**
	 * 
	 * This service is used to convert the record in the format in which zscore
	 * service requires.
	 */
	private JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> trendScoreService(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD,
			ZScoreService zScoreService) {
		logger.info("Preprocessing before executing zcore service");
		JavaPairRDD<String, DayScore> itemDayScoreRDD = preprocessingForZScore(
				cassandraRDD, zScoreService.getDaysForHistoricTrend(),
				zScoreService.getCurrentTrendDate());
		logger.info("Executing zscore algorithm");
		JavaRDD<TrendScore> itemTrendScoreRDD = zScoreService
				.calculateScore(itemDayScoreRDD);
		logger.info("Postprocessing after executing zscore service- preparing record in the format in which cassandra require");
		JavaRDD<TrendScore> normalizedItemTrendScoreRDD = zScoreService
				.normalizedScore(itemTrendScoreRDD);

		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = TrendRecoPostprocessing
				.processingForTrendScore(normalizedItemTrendScoreRDD);
		return cassandraOutputRDD;

	}

	private JavaPairRDD<String, DayScore> preprocessingForZScore(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD,
			int period, long currentDateTimestamp) {
		logger.info("Preprocessing ZScore: transformation of record");
		JavaPairRDD<String, DayScore> dayScoreRDD = DayScoreTransformation
				.getDayScoreWithKey(cassandraRDD);
		logger.info("Preprocessing ZScore: Filtering of record");
		JavaPairRDD<String, DayScore> filteredDayScoreRDD = Filter
				.filterDayScoreEvent(dayScoreRDD);
		return filteredDayScoreRDD;

	}

	public class TrendScoreConfig {
		public Date startDate;
		public Date endDate;
		public int periodForHistoryData;

		public TrendScoreConfig(Date startDate, Date endDate,
				int periodForHistoryData) {
			super();
			this.startDate = startDate;
			this.endDate = endDate;
			this.periodForHistoryData = periodForHistoryData;
		}

		public TrendScoreConfig(Date currentTrendDate, int periodForHistoryData) {
			this(currentTrendDate, currentTrendDate, periodForHistoryData);
		}
	}

	public static void main(String[] args) {
		TrendScoreDriver trendRecoSer = new TrendScoreDriver();
		if (args.length == 2) {
			trendRecoSer.runTrendScoreDriver(args[0], args[1]);
		} else {
			System.err.println("Please provide the path of property file");
		}
	}

}
