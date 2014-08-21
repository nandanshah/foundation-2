package com.dla.foundation.useritemreco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.useritemreco.model.ItemSummary;
import com.dla.foundation.useritemreco.model.Score;
import com.dla.foundation.useritemreco.model.ScoreType;
import com.dla.foundation.useritemreco.model.userItemRecoCF;

/**
 * This class is used to transform record of score summary in cassandra format
 * into required format
 * 
 * @author shishir_shivhare
 * 
 */
public class ScoreSummaryTransformation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2366511526756341991L;
	private static final String DELIMITER_PROPERTY = "#";
	private static final String NOT_AVAILABLE = "N/A";

	public static JavaPairRDD<String, ItemSummary> getScoreSummary(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {

		JavaPairRDD<String, ItemSummary> trendRDD = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, ItemSummary>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -5603807529160303375L;
					Map<String, Score> scores;
					ItemSummary itemSummary;

					public Tuple2<String, ItemSummary> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
									throws Exception {
						String tenantId = null;
						String regionId = null;
						String itemId = null;
						String primaryKey = null;
						Date date = null;
						scores = new HashMap<String, Score>();
						Map<String, ByteBuffer> priamryKeyColumns = record._1();
						if (priamryKeyColumns != null) {
							for (Entry<String, ByteBuffer> column : priamryKeyColumns
									.entrySet()) {

								if (column
										.getKey()
										.compareToIgnoreCase(
												userItemRecoCF.TENANT
												.getColumn()) == 0) {
									if (null != column.getValue())
										tenantId = UUIDType.instance.compose(
												column.getValue()).toString();

								} else if (column
										.getKey()
										.compareToIgnoreCase(
												userItemRecoCF.REGION
												.getColumn()) == 0) {
									if (null != column.getValue())
										regionId = UUIDType.instance.compose(
												column.getValue()).toString();

								} else if (column
										.getKey()
										.compareToIgnoreCase(
												userItemRecoCF.ITEM.getColumn()) == 0) {
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
												userItemRecoCF.TREND_SCORE
												.getColumn()) == 0) {

									if (null != column.getValue()) {
										if (scores
												.containsKey(ScoreType.TREND_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.TREND_TYPE
													.getColumn())
													.setScore(
															ByteBufferUtil
															.toDouble(column
																	.getValue()));
										} else {
											Score score = new Score();
											score.setScore(ByteBufferUtil
													.toDouble(column.getValue()));
											scores.put(ScoreType.TREND_TYPE
													.getColumn(), score);
										}

									} else {
										if (scores
												.containsKey(ScoreType.TREND_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.TREND_TYPE
													.getColumn())
													.setScore(0);
										} else {
											Score score = new Score();
											score.setScore(0);
											scores.put(ScoreType.TREND_TYPE
													.getColumn(), score);
										}
									}

								} else if (column
										.getKey()
										.compareToIgnoreCase(
												userItemRecoCF.TREND_SCORE_REASON
												.getColumn()) == 0) {

									if (null != column.getValue()) {
										if (scores
												.containsKey(ScoreType.TREND_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.TREND_TYPE
													.getColumn())
													.setScoreReason(
															ByteBufferUtil
															.string(column
																	.getValue()));
										} else {
											Score score = new Score();
											score.setScoreReason(ByteBufferUtil
													.string(column.getValue()));
											scores.put(ScoreType.TREND_TYPE
													.getColumn(), score);
										}

									} else {
										if (scores
												.containsKey(ScoreType.TREND_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.TREND_TYPE
													.getColumn())
													.setScoreReason(
															NOT_AVAILABLE);
										} else {
											Score score = new Score();
											score.setScoreReason(NOT_AVAILABLE);
											scores.put(ScoreType.TREND_TYPE
													.getColumn(), score);
										}
									}

								} else if (column
										.getKey()
										.compareToIgnoreCase(
												userItemRecoCF.POPULARITY_SCORE
												.getColumn()) == 0) {

									if (null != column.getValue()) {
										if (scores
												.containsKey(ScoreType.POPULARITY_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.POPULARITY_TYPE
													.getColumn())
													.setScore(
															ByteBufferUtil
															.toDouble(column
																	.getValue()));
										} else {
											Score score = new Score();
											score.setScore(ByteBufferUtil
													.toDouble(column.getValue()));
											scores.put(
													ScoreType.POPULARITY_TYPE
													.getColumn(), score);
										}

									} else {
										if (scores
												.containsKey(ScoreType.POPULARITY_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.POPULARITY_TYPE
													.getColumn())
													.setScore(0);
										} else {
											Score score = new Score();
											score.setScore(0);
											scores.put(
													ScoreType.POPULARITY_TYPE
													.getColumn(), score);
										}
									}

								} else if (column
										.getKey()
										.compareToIgnoreCase(
												userItemRecoCF.POPULARITY_SCORE_REASON
												.getColumn()) == 0) {

									if (null != column.getValue()) {
										if (scores
												.containsKey(ScoreType.POPULARITY_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.POPULARITY_TYPE
													.getColumn())
													.setScoreReason(
															ByteBufferUtil
															.string(column
																	.getValue()));
										} else {
											Score score = new Score();
											score.setScoreReason(ByteBufferUtil
													.string(column.getValue()));
											scores.put(
													ScoreType.POPULARITY_TYPE
													.getColumn(), score);
										}

									} else {
										if (scores
												.containsKey(ScoreType.POPULARITY_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.POPULARITY_TYPE
													.getColumn())
													.setScoreReason(
															NOT_AVAILABLE);
										} else {
											Score score = new Score();
											score.setScoreReason(NOT_AVAILABLE);
											scores.put(
													ScoreType.POPULARITY_TYPE
													.getColumn(), score);
										}
									}

								} 



								/*
								 * 
								 * 
								 */

								else if (column
										.getKey()
										.compareToIgnoreCase(
												userItemRecoCF.FP_SCORE
												.getColumn()) == 0) {

									if (null != column.getValue()) {
										if (scores
												.containsKey(ScoreType.FP_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.FP_TYPE
													.getColumn())
													.setScore(
															ByteBufferUtil
															.toDouble(column
																	.getValue()));
										} else {
											Score score = new Score();
											score.setScore(ByteBufferUtil
													.toDouble(column.getValue()));
											scores.put(
													ScoreType.FP_TYPE
													.getColumn(), score);
										}

									} else {
										if (scores
												.containsKey(ScoreType.FP_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.FP_TYPE
													.getColumn())
													.setScore(0);
										} else {
											Score score = new Score();
											score.setScore(0);
											scores.put(
													ScoreType.FP_TYPE
													.getColumn(), score);
										}
									}

								} else if (column
										.getKey()
										.compareToIgnoreCase(
												userItemRecoCF.FP_SCORE_REASON
												.getColumn()) == 0) {

									if (null != column.getValue()) {
										if (scores
												.containsKey(ScoreType.FP_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.FP_TYPE
													.getColumn())
													.setScoreReason(
															ByteBufferUtil
															.string(column
																	.getValue()));
										} else {
											Score score = new Score();
											score.setScoreReason(ByteBufferUtil
													.string(column.getValue()));
											scores.put(
													ScoreType.FP_TYPE
													.getColumn(), score);
										}

									} else {
										if (scores
												.containsKey(ScoreType.FP_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.FP_TYPE
													.getColumn())
													.setScoreReason(
															NOT_AVAILABLE);
										} else {
											Score score = new Score();
											score.setScoreReason(NOT_AVAILABLE);
											scores.put(
													ScoreType.FP_TYPE
													.getColumn(), score);
										}
									}

								}



								else if (column
										.getKey()
										.compareToIgnoreCase(
												userItemRecoCF.NEW_RELEASE_SCORE
												.getColumn()) == 0) {

									if (null != column.getValue()) {
										if (scores
												.containsKey(ScoreType.NEW_RELEASE_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.NEW_RELEASE_TYPE
													.getColumn())
													.setScore(
															ByteBufferUtil
															.toDouble(column
																	.getValue()));
										} else {
											Score score = new Score();
											score.setScore(ByteBufferUtil
													.toDouble(column.getValue()));
											scores.put(
													ScoreType.NEW_RELEASE_TYPE
													.getColumn(), score);
										}

									} else {
										if (scores
												.containsKey(ScoreType.NEW_RELEASE_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.NEW_RELEASE_TYPE
													.getColumn())
													.setScore(0);
										} else {
											Score score = new Score();
											score.setScore(0);
											scores.put(
													ScoreType.NEW_RELEASE_TYPE
													.getColumn(), score);
										}
									}

								} else if (column
										.getKey()
										.compareToIgnoreCase(
												userItemRecoCF.NEW_RELEASE_SCORE_REASON
												.getColumn()) == 0) {

									if (null != column.getValue()) {
										if (scores
												.containsKey(ScoreType.NEW_RELEASE_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.NEW_RELEASE_TYPE
													.getColumn())
													.setScoreReason(
															ByteBufferUtil
															.string(column
																	.getValue()));
										} else {
											Score score = new Score();
											score.setScoreReason(ByteBufferUtil
													.string(column.getValue()));
											scores.put(
													ScoreType.NEW_RELEASE_TYPE
													.getColumn(), score);
										}

									} else {
										if (scores
												.containsKey(ScoreType.NEW_RELEASE_TYPE
														.getColumn())) {
											scores.get(
													ScoreType.NEW_RELEASE_TYPE
													.getColumn())
													.setScoreReason(
															NOT_AVAILABLE);
										} else {
											Score score = new Score();
											score.setScoreReason(NOT_AVAILABLE);
											scores.put(
													ScoreType.NEW_RELEASE_TYPE
													.getColumn(), score);
										}
									}

								}

								/*
								 * 
								 * 
								 */

								else if (column
										.getKey()
										.compareToIgnoreCase(
												userItemRecoCF.DATE.getColumn()) == 0) {
									if (null != column.getValue())
										date = UserItemRecommendationUtil
										.processInputDate(TimestampType.instance
												.compose(column
														.getValue()));

								}
							}
						}
						if (null != tenantId && null != regionId
								&& null != itemId && null != scores
								&& null != date) {
							primaryKey = tenantId + DELIMITER_PROPERTY
									+ regionId;
							itemSummary = new ItemSummary(tenantId, regionId,
									itemId, scores, date);
							return new Tuple2<String, ItemSummary>(primaryKey,
									itemSummary);
						}
						return null;

					}
				});

		return trendRDD;
	}

}
