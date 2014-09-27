package com.dla.foundation.useritemreco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.useritemreco.model.ItemSummary;
import com.dla.foundation.useritemreco.model.Score;
import com.dla.foundation.useritemreco.model.ScoreType;
import com.dla.foundation.useritemreco.model.UserItemRecoCF;

/**
 * This class is used to transform record of trend, popularity in cassandra
 * format into required format. it will also provide support to other item level
 * column family in future
 * 
 * @author shishir_shivhare
 * 
 */
public class ItemSummaryTransformation implements Serializable {

	private static final long serialVersionUID = -1064163747960298948L;

	private static Logger logger = Logger
			.getLogger(ItemSummaryTransformation.class);

	private static final String DELIMITER_PROPERTY = "#";

	public static JavaPairRDD<String, ItemSummary> getItemSummary(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {
		JavaPairRDD<String, ItemSummary> itemSummaryRDD = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, ItemSummary>() {

					private static final long serialVersionUID = -4177767984118744330L;
					ItemSummary itemSummary;
					Score score;
					Map<String, Score> scores;

					public Tuple2<String, ItemSummary> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
							throws CharacterCodingException {

						String tenantId = null;
						String regionId = null;
						score = new Score();
						scores = new HashMap<String, Score>();
						String itemId = null;
						String primaryKey = null;
						Date date = null;
						Map<String, ByteBuffer> priamryKeyColumns = record._1();
						if (priamryKeyColumns != null) {
							for (Entry<String, ByteBuffer> column : priamryKeyColumns
									.entrySet()) {

								if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.TENANT.getColumn()) == 0) {
									if (null != column.getValue())
										tenantId = UUIDType.instance.compose(
												column.getValue()).toString();

								} else if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.REGION.getColumn()) == 0) {
									if (null != column.getValue())
										regionId = UUIDType.instance.compose(
												column.getValue()).toString();

								} else if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.ITEM.getColumn()) == 0) {
									if (null != column.getValue())
										itemId = UUIDType.instance.compose(
												column.getValue()).toString();

								}

							}
						}

						Map<String, ByteBuffer> otherColumns = record._2;
						if (otherColumns != null) {

							for (Entry<String, ByteBuffer> column : otherColumns
									.entrySet()) {
								if (column
										.getKey()
										.compareToIgnoreCase(
												UserItemRecoCF.NORMALIZED_POPULARITY_SCORE
														.getColumn()) == 0) {

									if (null != column.getValue()) {
										score.setScore(ByteBufferUtil
												.toDouble(column.getValue()));
										score.setType(ScoreType.POPULARITY_TYPE
												.getColumn());
									} else {
										return null;
									}

								} else if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.POPULARITY_SCORE_REASON
												.getColumn()) == 0) {
									if (null != column.getValue()) {
										score.setScoreReason(ByteBufferUtil
												.string(column.getValue()));
									} else {
										return null;
									}
								} else if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.NORMALIZED_TREND_SCORE
												.getColumn()) == 0) {

									if (null != column.getValue()) {
										score.setScore(ByteBufferUtil
												.toDouble(column.getValue()));
										score.setType(ScoreType.TREND_TYPE
												.getColumn());
									} else {
										return null;
									}

								} else if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.TREND_SCORE_REASON
												.getColumn()) == 0) {
									if (null != column.getValue()) {
										score.setScoreReason(ByteBufferUtil
												.string(column.getValue()));
									} else {
										return null;
									}

								}

								else if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.NORMALIZED_NEW_SCORE
												.getColumn()) == 0) {

									if (null != column.getValue()) {
										score.setScore(ByteBufferUtil
												.toDouble(column.getValue()));
										score.setType(ScoreType.NEW_RELEASE_TYPE
												.getColumn());
									} else {
										return null;
									}

								} else if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.NEW_RELEASE_SCORE_REASON
												.getColumn()) == 0) {
									if (null != column.getValue()) {
										score.setScoreReason(ByteBufferUtil
												.string(column.getValue()));
									} else {
										return null;
									}

								}

								else if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.DATE.getColumn()) == 0) {
									if (null != column.getValue()) {
										date = UserItemRecommendationUtil
												.processInputDate(TimestampType.instance
														.compose(column
																.getValue()));
									} else {
										return null;
									}

								}
							}
						}
						if (null != tenantId && null != regionId
								&& null != itemId && null != score
								&& -1 != score.getScore()) {
							primaryKey = tenantId + DELIMITER_PROPERTY
									+ regionId + DELIMITER_PROPERTY + itemId;
							scores.put(score.getType(), score);
							itemSummary = new ItemSummary(tenantId, regionId,
									itemId, scores, date);
							logger.debug("Transforming scores at item level tenantId :"
									+ tenantId
									+ " regionId :"
									+ regionId
									+ " itemId :" + itemId);
							return new Tuple2<String, ItemSummary>(primaryKey,
									itemSummary);
						}
						return null;

					}
				});

		return itemSummaryRDD;
	}

}
