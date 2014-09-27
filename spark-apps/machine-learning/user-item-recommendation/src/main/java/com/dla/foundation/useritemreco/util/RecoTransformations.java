package com.dla.foundation.useritemreco.util;

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
import com.dla.foundation.useritemreco.model.UserItemSummary;
import com.dla.foundation.useritemreco.model.UserItemRecoCF;

public class RecoTransformations {
	private static final String DELIMITER_PROPERTY = "#";
	private static Logger logger = Logger
			.getLogger(ScoreSummaryTransformation.class);

	public static JavaPairRDD<String, UserItemSummary> getTransformations(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {
		JavaPairRDD<String, UserItemSummary> profileRDD = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, UserItemSummary>() {

					private static final long serialVersionUID = -5603807529160303375L;
					Score score;
					Map<String, Score> scores;
					ItemSummary itemSummary;
					UserItemSummary userItemSummary;

					public Tuple2<String, UserItemSummary> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
							throws CharacterCodingException {
						String tenantId = null;
						String regionId = null;
						String userId = null;
						String itemId = null;
						Date date = null;
						score = new Score();

						scores = new HashMap<String, Score>();

						String primaryKey = null;
						Map<String, ByteBuffer> priamryKeyColumns = record._1();
						if (priamryKeyColumns != null) {
							for (Entry<String, ByteBuffer> column : priamryKeyColumns
									.entrySet()) {

								if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.TENANT.getColumn()) == 0) {
									if (null != column.getValue())
										tenantId = UUIDType.instance.compose(
												column.getValue()).toString();

								}

								if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.REGION.getColumn()) == 0) {
									if (null != column.getValue())
										regionId = UUIDType.instance.compose(
												column.getValue()).toString();

								}

								if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.PROFILE.getColumn()) == 0) {
									if (null != column.getValue())
										userId = UUIDType.instance.compose(
												column.getValue()).toString();

								}

								if (column.getKey().compareToIgnoreCase(
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
								if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.TENANT.getColumn()) == 0) {
									if (null != column.getValue())
										tenantId = UUIDType.instance.compose(
												column.getValue()).toString();
									else {
										return null;
									}
								}

								if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.REGION.getColumn()) == 0) {
									if (null != column.getValue())
										regionId = UUIDType.instance.compose(
												column.getValue()).toString();
									else {
										return null;
									}
								}
								if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.SOCIAL_SCORE_REASON
												.getColumn()) == 0) {
									if (null != column.getValue()) {
										score.setScoreReason(ByteBufferUtil
												.string(column.getValue()));

										score.setType(ScoreType.SOCIAL_TYPE
												.getColumn());

									}

								}
								if (column.getKey()
										.compareToIgnoreCase(
												UserItemRecoCF.SOCIAL_SCORE
														.getColumn()) == 0) {
									if (null != column.getValue())
										score.setScore((ByteBufferUtil
												.toDouble(column.getValue())));

								}
								if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.PIO_SCORE_REASON
												.getColumn()) == 0) {
									if (null != column.getValue()) {
										score.setScoreReason((ByteBufferUtil
												.string(column.getValue())));
										score.setType(ScoreType.PIO_TYPE
												.getColumn());
									}
								}
								if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.PIO_SCORE.getColumn()) == 0) {
									if (null != column.getValue())
										score.setScore((ByteBufferUtil
												.toDouble(column.getValue())));

								}

								if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.DATE.getColumn()) == 0) {
									if (null != column.getValue())
										date = UserItemRecommendationUtil
												.processInputDate(TimestampType.instance
														.compose(column
																.getValue()));
								}
								if (column.getKey().compareToIgnoreCase(
										UserItemRecoCF.PIO_DATE.getColumn()) == 0) {
									if (null != column.getValue())
										date = UserItemRecommendationUtil
												.processInputDate(TimestampType.instance
														.compose(column
																.getValue()));
								}
							}
						}
						if (null != itemId && null != tenantId
								&& null != regionId && null != userId) {
							primaryKey = tenantId + DELIMITER_PROPERTY
									+ regionId + DELIMITER_PROPERTY + itemId
									+ DELIMITER_PROPERTY + userId;
							scores.put(score.getType(), score);

							itemSummary = new ItemSummary(tenantId, regionId,
									itemId, scores, date);
							userItemSummary = new UserItemSummary(userId,
									itemSummary);
							logger.debug("Getting all details from profile & ScoreSummary tenantId :"
									+ tenantId
									+ " regionId :"
									+ regionId
									+ " itemId :"
									+ itemId
									+ " userId :"
									+ userId);
							return new Tuple2<String, UserItemSummary>(
									primaryKey, userItemSummary);

						}
						return null;

					}
				});

		return profileRDD;
	}

}
