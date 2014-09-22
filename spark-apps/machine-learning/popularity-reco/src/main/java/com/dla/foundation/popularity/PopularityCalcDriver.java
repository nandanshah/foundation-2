package com.dla.foundation.popularity;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import com.datastax.driver.core.utils.UUIDs;
import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.popularity.exception.InvalidDataException;
import com.dla.foundation.popularity.utils.CassandraConfig;
import com.dla.foundation.popularity.utils.ColumnCollections;
import com.dla.foundation.popularity.utils.PopularityConstants;

public class PopularityCalcDriver implements Serializable {
	private static final long serialVersionUID = -4763334583151269669L;
	private static final String EQUALITY_STRING = "date = ";
	private static final String VALUE_DELIMETER = "#";
	private final int RECORD_INC = 1;
	private Date executionDate;
	private static Logger logger = Logger.getLogger(PopularityCalcDriver.class
			.getName());

	public void popularityCalc(JavaSparkContext javaSparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			CassandraConfig cassandraConfig, Date date)
			throws InvalidDataException {

		Accumulator<Integer> accumEventSummaryRecords = javaSparkContext
				.accumulator(0);
		Accumulator<Integer> accumPopularityRecords = javaSparkContext
				.accumulator(0);
		executionDate = DateUtils.addDays(date, -1);
		logger.info("Started reading dayScores from "
				+ cassandraConfig.analyticsKeySpace + "."
				+ PopularityConstants.EVENT_SUMMARY_CF + " for Date "
				+ executionDate);
		JavaPairRDD<String, String> dayScoreSummary = getDayScoreSummary(
				javaSparkContext, cassandraSparkConnector, cassandraConfig,
				accumEventSummaryRecords);
		logger.info("Completed reading dayScores from "
				+ cassandraConfig.analyticsKeySpace + "."
				+ PopularityConstants.EVENT_SUMMARY_CF + " for Date "
				+ executionDate);
		logger.info("Started reading popularity data from "
				+ cassandraConfig.analyticsKeySpace + "."
				+ PopularityConstants.POPULARITY_CF + " for Date "
				+ executionDate);
		JavaPairRDD<String, String> popularityScore = getPopularityScore(
				javaSparkContext, cassandraSparkConnector, cassandraConfig,
				accumPopularityRecords);
		logger.info("Completed reading popularity data from "
				+ cassandraConfig.analyticsKeySpace + "."
				+ PopularityConstants.POPULARITY_CF + " for Date "
				+ executionDate);
		logger.info("Started calculating aggregate popularity scores.");

		JavaPairRDD<String, String> aggregatePopularityScores = getAggregatePopularityScore(
				javaSparkContext, dayScoreSummary, popularityScore);
		logger.info("Completed calculating aggregate popularity scores.");
		logger.info("Started calculating normalized popularity scores.");
		JavaPairRDD<String, String> normalizedPopularityScores = getNormalizedPopularityScores(aggregatePopularityScores);
		if (accumEventSummaryRecords.value() <= 0) {
			throw new InvalidDataException(accumEventSummaryRecords.value()
					+ "No day-scores data available for date : "
					+ executionDate
					+ ". Please make sure data is available for date '"
					+ executionDate + "' in "
					+ PopularityConstants.EVENT_SUMMARY_CF);
		}
		if (((accumPopularityRecords.value() <= 0
				&& PopularityClient.executionMode
						.equalsIgnoreCase(PopularityConstants.INCREMENTAL) && !PopularityClient.firstExecution) || (accumPopularityRecords
				.value() <= 0
				&& PopularityClient.executionMode
						.equalsIgnoreCase(PopularityConstants.FULLCOMPUTE) && PopularityClient.updatedModetoFullCompute))) {
			throw new InvalidDataException(
					"No popularuty data available in Cassandra for Date : "
							+ executionDate + ". You can retry running in '"
							+ PopularityConstants.FULLCOMPUTE + "' mode.");
		}

		logger.info("Completed calculating normalized popularity scores.");
		logger.info("Started writting calculated popularity scores to "
				+ cassandraConfig.analyticsKeySpace + "."
				+ PopularityConstants.POPULARITY_CF);
		writePopularityToCassandra(normalizedPopularityScores,
				cassandraSparkConnector, cassandraConfig);
		logger.info("Completed writting calculated popularity scores to "
				+ cassandraConfig.analyticsKeySpace + "."
				+ PopularityConstants.POPULARITY_CF);

		logger.info("Poplarity Reco has considered "
				+ accumPopularityRecords.value() + " records from "
				+ PopularityConstants.POPULARITY_CF + " with Date " + date);
		logger.info("Poplarity Reco has considered "
				+ accumEventSummaryRecords.value() + " records from "
				+ PopularityConstants.EVENT_SUMMARY_CF + " with Date " + date);
	}

	private JavaPairRDD<String, String> getDayScoreSummary(
			JavaSparkContext javaSparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			CassandraConfig cassandraConfig,
			final Accumulator<Integer> accumEventSummaryRecords)
			throws InvalidDataException {

		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraDayScore = cassandraSparkConnector
				.read(new Configuration(),
						javaSparkContext,
						cassandraConfig.analyticsKeySpace,
						PopularityConstants.EVENT_SUMMARY_CF,
						cassandraConfig.pageRowSize,
						EQUALITY_STRING
								+ PopularityClient
										.getFormattedDate(executionDate
												.getTime()));
		logger.info("Started pushing dayScores data into RDDs");
		JavaPairRDD<String, String> dayScores = cassandraDayScore
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
							throws Exception {
						Map<String, ByteBuffer> keyMap = record._1;
						Map<String, ByteBuffer> valueMap = record._2;

						ByteBuffer bytesItemid = keyMap
								.get(ColumnCollections.ITEM_ID);
						ByteBuffer bytesTenantid = keyMap
								.get(ColumnCollections.TENANT_ID);
						ByteBuffer bytesRegionid = keyMap
								.get(ColumnCollections.REGION_ID);
						String valueDayScore = Double.toString(ByteBufferUtil
								.toDouble(valueMap
										.get(ColumnCollections.DAY_SCORE)));
						accumEventSummaryRecords.add(RECORD_INC);
						String key = UUIDType.instance
								.compose(bytesItemid)
								.toString()
								.concat(VALUE_DELIMETER)
								.concat(UUIDType.instance
										.compose(bytesTenantid).toString())
								.concat(VALUE_DELIMETER)
								.concat(UUIDType.instance
										.compose(bytesRegionid).toString());

						return new Tuple2<String, String>(key, valueDayScore);
					}

				});

		return dayScores;
	}

	private JavaPairRDD<String, String> getPopularityScore(
			JavaSparkContext javaSparkContext,
			CassandraSparkConnector cassandraSparkConnector,
			CassandraConfig cassandraConfig,
			final Accumulator<Integer> accumPopularityRecords)
			throws InvalidDataException {
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraPopularityScore = cassandraSparkConnector
				.read(new Configuration(),
						javaSparkContext,
						cassandraConfig.analyticsKeySpace,
						PopularityConstants.POPULARITY_CF,
						cassandraConfig.pageRowSize,
						EQUALITY_STRING
								+ PopularityClient
										.getFormattedDate(executionDate
												.getTime()));

		logger.info("Started pushing popularity data into RDDs");
		JavaPairRDD<String, String> popularityScore = cassandraPopularityScore
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, String>() {

					private static final long serialVersionUID = 5754328997329065127L;

					@Override
					public Tuple2<String, String> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
							throws Exception {
						Map<String, ByteBuffer> keyMap = record._1;
						Map<String, ByteBuffer> valueMap = record._2;
						ByteBuffer bytesItemid = keyMap
								.get(ColumnCollections.ITEM_ID);
						ByteBuffer bytesTenantid = keyMap
								.get(ColumnCollections.TENANT_ID);
						ByteBuffer bytesRegionid = keyMap
								.get(ColumnCollections.REGION_ID);
						String popularityScore = Double.toString(ByteBufferUtil.toDouble(valueMap
								.get(ColumnCollections.POPULARITY_SCORE)));
						accumPopularityRecords.add(RECORD_INC);
						String key = UUIDType.instance
								.compose(bytesItemid)
								.toString()
								.concat(VALUE_DELIMETER)
								.concat(UUIDType.instance
										.compose(bytesTenantid).toString())
								.concat(VALUE_DELIMETER)
								.concat(UUIDType.instance
										.compose(bytesRegionid).toString());

						return new Tuple2<String, String>(key, popularityScore);

					}
				});
		return popularityScore;

	}

	private JavaPairRDD<String, String> getAggregatePopularityScore(
			JavaSparkContext javaSparkContext,
			JavaPairRDD<String, String> dayScoreSummary,
			JavaPairRDD<String, String> popularityScore) {
		@SuppressWarnings("unchecked")
		JavaPairRDD<String, String> aggregatePopularityScores = javaSparkContext
				.union(dayScoreSummary, popularityScore);
		return aggregatePopularityScores
				.reduceByKey(new Function2<String, String, String>() {

					private static final long serialVersionUID = -7540499574281268030L;

					@Override
					public String call(String dayScore, String popularityScore)
							throws Exception {
						return Double.toString(Double.parseDouble(dayScore)
								+ Double.parseDouble(popularityScore));
					}
				});

	}

	private JavaPairRDD<String, String> getNormalizedPopularityScores(
			JavaPairRDD<String, String> aggregatePopularityScores) {
		aggregatePopularityScores.persist(StorageLevel.MEMORY_AND_DISK());
		final Double MAX_AGGREGATE_SCORE = findMaxAggregateScore(aggregatePopularityScores);
		logger.info("Calculated MAX_AGGREGATE_SCORE is :" + MAX_AGGREGATE_SCORE);
		logger.info("Calculating normalized popularity scores on basis of MAX_AGGREGATE_SCORE");
		JavaPairRDD<String, String> normalizedPopularityScore = aggregatePopularityScores
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

					@Override
					public Tuple2<String, String> call(
							Tuple2<String, String> record) throws Exception {
						double aggregatePopularityScore = Double
								.parseDouble(record._2);
						double normalizedPopularityScore = aggregatePopularityScore
								/ MAX_AGGREGATE_SCORE;
						return new Tuple2<String, String>(record._1, String
								.valueOf(aggregatePopularityScore)
								.concat(VALUE_DELIMETER)
								.concat(String
										.valueOf(normalizedPopularityScore)));
					}
				});

		return normalizedPopularityScore;

	}

	private Double findMaxAggregateScore(
			JavaPairRDD<String, String> aggregatePopularityScores) {
		JavaRDD<Double> allScoresRDD = aggregatePopularityScores
				.map(new Function<Tuple2<String, String>, Double>() {

					@Override
					public Double call(Tuple2<String, String> records)
							throws Exception {
						return Double.valueOf(records._2);
					}
				});
		return allScoresRDD.reduce(new Function2<Double, Double, Double>() {

			@Override
			public Double call(Double val1, Double val2) throws Exception {
				if (val1 > val2)
					return val1;
				else
					return val2;
			}
		});
	}

	private void writePopularityToCassandra(
			JavaPairRDD<String, String> normalizedPopularityScores,
			CassandraSparkConnector cassandraSparkConnector,
			CassandraConfig cassandraConfig) {

		String query = "UPDATE " + cassandraConfig.analyticsKeySpace + "."
				+ PopularityConstants.POPULARITY_CF + " SET "
				+ ColumnCollections.LAST_MODIFIED + "=?, "
				+ ColumnCollections.EVENT_REQUIRED + "=?, "
				+ ColumnCollections.NORMALIZED_SCORE + "=?, "
				+ ColumnCollections.POPULARITY_SCORE + "=?, "
				+ ColumnCollections.RECO_REASON + "=?";
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraWriteRDD = normalizedPopularityScores
				.mapToPair(new PairFunction<Tuple2<String, String>, Map<String, ByteBuffer>, List<ByteBuffer>>() {

					@Override
					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							Tuple2<String, String> record) throws Exception {
						Map<String, ByteBuffer> keyMap = new HashMap<String, ByteBuffer>();
						List<ByteBuffer> valueList = new ArrayList<ByteBuffer>();
						long updateTime = PopularityClient
								.getFormattedDate(DateUtils.addDays(
										executionDate, 1).getTime());
						Date updateDate = new Date(PopularityClient
								.getFormattedDate(updateTime));
						ByteBuffer periodid = UUIDType.instance.decompose(UUIDs
								.startOf((updateTime)));
						String[] keysString = record._1.split(VALUE_DELIMETER);
						String[] valuesString = record._2
								.split(VALUE_DELIMETER);
						String strItemId = keysString[0];
						String strTenantId = keysString[1];
						String strRegionId = keysString[2];
						String strAggregatePopularity = valuesString[0];
						String strNormalizedPopularity = valuesString[1];
						keyMap.put(ColumnCollections.PERIOD_ID, periodid);
						keyMap.put(ColumnCollections.ITEM_ID, UUIDType.instance
								.decompose(UUID.fromString(strItemId)));
						keyMap.put(ColumnCollections.TENANT_ID,
								UUIDType.instance.decompose(UUID
										.fromString(strTenantId)));
						keyMap.put(ColumnCollections.REGION_ID,
								UUIDType.instance.decompose(UUID
										.fromString(strRegionId)));
						valueList.add(TimestampType.instance
								.decompose(updateDate));
						valueList.add(ByteBufferUtil
								.bytes(PopularityConstants.EVENT_REQUIRED));
						valueList.add(ByteBufferUtil.bytes(Double
								.parseDouble(strNormalizedPopularity)));
						valueList.add(ByteBufferUtil.bytes(Double
								.parseDouble(strAggregatePopularity)));
						valueList.add(ByteBufferUtil
								.bytes(PopularityConstants.RECO_REASON));
						return new Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>(
								keyMap, valueList);
					}

				});
		cassandraSparkConnector.write(new Configuration(),
				cassandraConfig.analyticsKeySpace,
				PopularityConstants.POPULARITY_CF, query, cassandraWriteRDD);

	}

}
