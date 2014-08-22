package com.dla.foundation.trendReco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.datastax.driver.core.utils.UUIDs;
import com.dla.foundation.trendReco.model.DailyEventSummaryPerItem;
import com.dla.foundation.trendReco.model.DailyEventSummaryPerUserItem;
import com.dla.foundation.trendReco.model.DayScore;
import com.dla.foundation.trendReco.model.Trend;
import com.dla.foundation.trendReco.model.TrendScore;
import com.dla.foundation.trendReco.model.UserSummary;

/**
 * This class provides the utility to convert the record in the format in which
 * spark-cassandra connector requires.
 * 
 * @author shishir_shivhare
 * 
 */
public class TrendRecoPostprocessing implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6629019652932514108L;
	private static final Integer REQUIRED_EVENT_VALUE = 1;
	private static final Integer EVENTREQUIRED = 1;
	private static final String TRENDING = "trending";

	public static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> processingForDayScore(
			JavaRDD<DayScore> dayScoreEventRDD) {
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = dayScoreEventRDD
				.mapToPair(new PairFunction<DayScore, Map<String, ByteBuffer>, List<ByteBuffer>>() {
					/**
			 * 
			 */
					private static final long serialVersionUID = 8819507645398525927L;
					Map<String, ByteBuffer> primaryKey;
					List<ByteBuffer> otherColumns;
					TimestampType timestampType;
					Map<String, Integer> eventTypeAggregate;

					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							DayScore eventWithDayScore) throws Exception {
						timestampType = TimestampType.instance;
						primaryKey = new LinkedHashMap<String, ByteBuffer>();
						eventTypeAggregate = eventWithDayScore.getEventTypeAggregate();
						otherColumns = new ArrayList<ByteBuffer>();

						primaryKey.put(DailyEventSummaryPerItem.PERIOD
								.getColumn(), ByteBufferUtil.bytes(UUIDs
								.startOf(eventWithDayScore.getTimestamp())));
						primaryKey.put(DailyEventSummaryPerItem.TENANT
								.getColumn(), UUIDType.instance
								.fromString(eventWithDayScore.getTenantId()));
						primaryKey.put(DailyEventSummaryPerItem.REGION
								.getColumn(), UUIDType.instance
								.fromString(eventWithDayScore.getRegionId()));
						primaryKey.put(DailyEventSummaryPerItem.ITEM
								.getColumn(), UUIDType.instance
								.fromString(eventWithDayScore.getItemId()));
						otherColumns.add(MapType.getInstance(UTF8Type.instance,
								Int32Type.instance).decompose(
								eventTypeAggregate));
						otherColumns.add(ByteBufferUtil.bytes(eventWithDayScore
								.getDayScore()));
						otherColumns.add(timestampType.decompose(new Date(
								eventWithDayScore.getTimestamp())));
						otherColumns.add(ByteBufferUtil
								.bytes(REQUIRED_EVENT_VALUE));
						return new Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>(
								primaryKey, otherColumns);
					}
				});
		return cassandraOutputRDD;
	}

	public static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> processingForTrendScore(
			JavaRDD<TrendScore> itemTrendScoreRDD) {
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = itemTrendScoreRDD
				.mapToPair(new PairFunction<TrendScore, Map<String, ByteBuffer>, List<ByteBuffer>>() {

					/**
			 * 
			 */
					private static final long serialVersionUID = -5791118052720872061L;
					Map<String, ByteBuffer> primaryKey;
					List<ByteBuffer> otherColumns;

					TimestampType timestampType;

					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							TrendScore itemTrendScore) throws Exception {

						timestampType = TimestampType.instance;

						primaryKey = new LinkedHashMap<String, ByteBuffer>();
						otherColumns = new ArrayList<ByteBuffer>();
						primaryKey.put(Trend.ID.getColumn(), ByteBufferUtil
								.bytes(UUIDs.startOf(TrendRecommendationUtil
										.getFormattedDate(itemTrendScore
												.getTimestamp()))));
						primaryKey.put(Trend.TENANT.getColumn(),
								UUIDType.instance.fromString(itemTrendScore
										.getTenantId()));
						primaryKey.put(Trend.REGION.getColumn(),
								UUIDType.instance.fromString(itemTrendScore
										.getRegionId()));
						primaryKey.put(Trend.ITEM.getColumn(),
								UUIDType.instance.fromString(itemTrendScore
										.getItemId()));
						otherColumns.add(ByteBufferUtil.bytes(itemTrendScore
								.getTrendScore()));
						otherColumns.add(ByteBufferUtil.bytes(itemTrendScore
								.getNormalizedScore()));
						otherColumns.add(ByteBufferUtil.bytes(TRENDING));
						otherColumns.add(timestampType.decompose(new Date(
								itemTrendScore.getTimestamp()))); 
						otherColumns.add(ByteBufferUtil.bytes(EVENTREQUIRED)); 

						return new Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>(
								primaryKey, otherColumns);

					}
				});

		return cassandraOutputRDD;
	}

	public static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> processingForUserSummary(
			JavaRDD<UserSummary> dayScoreEventRDD) {
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = dayScoreEventRDD
				.mapToPair(new PairFunction<UserSummary, Map<String, ByteBuffer>, List<ByteBuffer>>() {
					/**
			 * 
			 */
					private static final long serialVersionUID = 8819507645398525927L;
					Map<String, ByteBuffer> primaryKey;
					List<ByteBuffer> otherColumns;
					
					TimestampType timestampType;

					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							UserSummary userSummary) throws Exception {
					
						timestampType = TimestampType.instance;
						primaryKey = new LinkedHashMap<String, ByteBuffer>();
						otherColumns = new ArrayList<ByteBuffer>();
					
						primaryKey.put(DailyEventSummaryPerUserItem.PERIOD
								.getColumn(), ByteBufferUtil.bytes(UUIDs
								.startOf(userSummary.getTimestamp())));
						primaryKey.put(DailyEventSummaryPerUserItem.TENANT
								.getColumn(), UUIDType.instance
								.fromString(userSummary.getTenantId()));
						primaryKey.put(DailyEventSummaryPerUserItem.REGION
								.getColumn(), UUIDType.instance
								.fromString(userSummary.getRegionId()));
						primaryKey.put(DailyEventSummaryPerUserItem.ITEM
								.getColumn(), UUIDType.instance
								.fromString(userSummary.getItemId()));
						primaryKey.put(DailyEventSummaryPerUserItem.PROFILE
								.getColumn(), UUIDType.instance
								.fromString(userSummary.getUserId()));
						otherColumns.add(MapType.getInstance(UTF8Type.instance,
								Int32Type.instance).decompose(
								userSummary.getEventTypeAggregate()));
						otherColumns.add(ByteBufferUtil.bytes(userSummary
								.getDayScore()));
						otherColumns.add(timestampType.decompose(new Date(
								userSummary.getTimestamp())));
						otherColumns.add(ByteBufferUtil
								.bytes(REQUIRED_EVENT_VALUE));
						return new Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>(
								primaryKey, otherColumns);
					}
				});
		return cassandraOutputRDD;
	}

}
