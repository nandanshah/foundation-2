package com.dla.foundation.useritemreco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.datastax.driver.core.utils.UUIDs;
import com.dla.foundation.useritemreco.model.ItemSummary;
import com.dla.foundation.useritemreco.model.Score;
import com.dla.foundation.useritemreco.model.ScoreType;
import com.dla.foundation.useritemreco.model.UserItemSummary;
import com.dla.foundation.useritemreco.model.userItemRecoCF;

/**
 * This class is used to convert record in cassandra format before writing to any cassandra column family
 * @author shishir_shivhare
 *
 */
public class UserItemRecoPostprocess implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6629019652932514108L;
	private static final Integer REQUIRED_EVENT_VALUE = 1;

	public static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> processScoreSummary(
			JavaRDD<ItemSummary> scoreSummaryRDD) {
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = scoreSummaryRDD
				.mapToPair(new PairFunction<ItemSummary, Map<String, ByteBuffer>, List<ByteBuffer>>() {
					/**
			 * 
			 */
					private static final long serialVersionUID = -2501507294271649462L;
					Map<String, ByteBuffer> primaryKey;
					List<ByteBuffer> otherColumns;

					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							ItemSummary record) throws Exception {
						primaryKey = new LinkedHashMap<String, ByteBuffer>();
						otherColumns = new ArrayList<ByteBuffer>();
						Map<String, Score> scores = record.getScores();
						primaryKey.put(userItemRecoCF.ID.getColumn(),
								ByteBufferUtil.bytes(UUIDs.startOf(record
										.getDate().getTime())));
						primaryKey.put(userItemRecoCF.TENANT.getColumn(),
								UUIDType.instance.fromString(record
										.getTenantId()));
						primaryKey.put(userItemRecoCF.REGION.getColumn(),
								UUIDType.instance.fromString(record
										.getRegionId()));
						primaryKey.put(
								userItemRecoCF.ITEM.getColumn(),
								UUIDType.instance.fromString(record.getItemId()));
						otherColumns.add(ByteBufferUtil.bytes(scores.get(
								ScoreType.TREND_TYPE.getColumn()).getScore()));
						otherColumns.add(ByteBufferUtil.bytes(scores.get(
								ScoreType.TREND_TYPE.getColumn())
								.getScoreReason()));
						otherColumns.add(ByteBufferUtil.bytes(scores.get(
								ScoreType.POPULARITY_TYPE.getColumn())
								.getScore()));
						otherColumns.add(ByteBufferUtil.bytes(scores.get(
								ScoreType.POPULARITY_TYPE.getColumn())
								.getScoreReason()));
						otherColumns.add(ByteBufferUtil
								.bytes(UserItemRecommendationUtil
										.getFormattedDate(record.getDate()
												.getTime())));
						otherColumns.add(ByteBufferUtil
								.bytes(REQUIRED_EVENT_VALUE));
						return new Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>(
								primaryKey, otherColumns);
					}
				});

		return cassandraOutputRDD;
	}

	public static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> processUserItemScoreSummary(
			JavaRDD<UserItemSummary> userItemScoreRDD) {

		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraOutputRDD = userItemScoreRDD
				.mapToPair(new PairFunction<UserItemSummary, Map<String, ByteBuffer>, List<ByteBuffer>>() {
					/**
			 * 
			 */
					private static final long serialVersionUID = 8143660714228626917L;
					Map<String, ByteBuffer> primaryKey;
					List<ByteBuffer> otherColumns;
					Map<String, Score> scores;

					public Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>> call(
							UserItemSummary record) throws Exception {
						primaryKey = new LinkedHashMap<String, ByteBuffer>();
						otherColumns = new ArrayList<ByteBuffer>();

						scores = record.getItemSummary().getScores();
						primaryKey.put(
								userItemRecoCF.ID.getColumn(),
								ByteBufferUtil.bytes(UUIDs.startOf(record
										.getItemSummary().getDate().getTime())));
						primaryKey.put(userItemRecoCF.TENANT.getColumn(),
								UUIDType.instance.fromString(record
										.getItemSummary().getTenantId()));
						primaryKey.put(userItemRecoCF.REGION.getColumn(),
								UUIDType.instance.fromString(record
										.getItemSummary().getRegionId()));
						primaryKey.put(userItemRecoCF.ITEM.getColumn(),
								UUIDType.instance.fromString(record
										.getItemSummary().getItemId()));
						primaryKey.put(
								userItemRecoCF.USER_ID.getColumn(),
								UUIDType.instance.fromString(record.getUserId()));
						otherColumns.add(ByteBufferUtil.bytes(scores.get(
								ScoreType.TREND_TYPE.getColumn()).getScore()));
						otherColumns.add(ByteBufferUtil.bytes(scores.get(
								ScoreType.TREND_TYPE.getColumn())
								.getScoreReason()));
						otherColumns.add(ByteBufferUtil.bytes(scores.get(
								ScoreType.POPULARITY_TYPE.getColumn())
								.getScore()));
						otherColumns.add(ByteBufferUtil.bytes(scores.get(
								ScoreType.POPULARITY_TYPE.getColumn())
								.getScoreReason()));
						otherColumns.add(ByteBufferUtil
								.bytes(UserItemRecommendationUtil
										.getFormattedDate(record
												.getItemSummary().getDate()
												.getTime())));
						otherColumns.add(ByteBufferUtil
								.bytes(REQUIRED_EVENT_VALUE));
						return new Tuple2<Map<String, ByteBuffer>, List<ByteBuffer>>(
								primaryKey, otherColumns);
					}
				});

		return cassandraOutputRDD;
	}

}
