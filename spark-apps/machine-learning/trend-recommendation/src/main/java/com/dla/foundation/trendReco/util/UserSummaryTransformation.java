package com.dla.foundation.trendReco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.trendReco.model.DailyEventSummaryPerUserItem;
import com.dla.foundation.trendReco.model.UserSummary;

/**
 * This class provides the utility to convert the record (format provided
 * through spark-cassandra connector) to user Summary Event.
 * 
 * @author shishir_shivhare
 * 
 */
public class UserSummaryTransformation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6339194742636478273L;

	private static final String DELIMITER_PROPERTY = "#";

	public static JavaPairRDD<String, UserSummary> getUserSummaryWithKey(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {

		JavaPairRDD<String, UserSummary> userSummaryRDD = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, UserSummary>() {
					/**
			 * 
			 */
					private static final long serialVersionUID = 5374973368305114892L;
					UserSummary userSummary = null;

					public Tuple2<String, UserSummary> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
							throws Exception {

						String primaryKey = "";
						userSummary = getDayScorePerUser(record);
						if (null != userSummary) {
							primaryKey = primaryKey.concat(userSummary
									.getTenantId()
									+ DELIMITER_PROPERTY
									+ userSummary.getRegionId()
									+ DELIMITER_PROPERTY
									+ userSummary.getItemId()
									+ DELIMITER_PROPERTY
									+ userSummary.getTimestamp());

							if (0 != "".compareTo(primaryKey)) {
								return new Tuple2<String, UserSummary>(
										primaryKey, userSummary);
							}

						}
						return null;

					}
				});

		return userSummaryRDD;
	}

	private static UserSummary getDayScorePerUser(
			Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
			throws NumberFormatException, CharacterCodingException {
		UserSummary userSummary = new UserSummary();
		Map<String, ByteBuffer> priamryKeyColumns = record._1();
		if (priamryKeyColumns != null) {
			for (Entry<String, ByteBuffer> column : priamryKeyColumns
					.entrySet()) {

				if (column.getKey().compareToIgnoreCase(
						DailyEventSummaryPerUserItem.TENANT.getColumn()) == 0) {
					if (null != column.getValue())
						userSummary.setTenantId(UUIDType.instance.compose(
								column.getValue()).toString());

				} else if (column.getKey().compareToIgnoreCase(
						DailyEventSummaryPerUserItem.REGION.getColumn()) == 0) {
					if (null != column.getValue())
						userSummary.setRegionId(UUIDType.instance.compose(
								column.getValue()).toString());

				} else if (column.getKey().compareToIgnoreCase(
						DailyEventSummaryPerUserItem.ITEM.getColumn()) == 0) {
					if (null != column.getValue())
						userSummary.setItemId(UUIDType.instance.compose(
								column.getValue()).toString());

				}

			}
		}
		Map<String, ByteBuffer> otherColumns = record._2;
		if (otherColumns != null) {

			for (Entry<String, ByteBuffer> column : otherColumns.entrySet()) {
				if (column.getKey().compareToIgnoreCase(
						DailyEventSummaryPerUserItem.DAY_SCORE.getColumn()) == 0) {
					if (null != column.getValue())
						userSummary.setDayScore(ByteBufferUtil.toDouble(column
								.getValue()));
				} else if (column.getKey().compareToIgnoreCase(
						DailyEventSummaryPerUserItem.DATE.getColumn()) == 0) {
					if (null != column.getValue())

						userSummary.setTimestamp(TrendRecommendationUtil
								.getFormattedDate(TimestampType.instance
										.compose(column.getValue()).getTime()));

				} else if (column.getKey().compareToIgnoreCase(
						DailyEventSummaryPerUserItem.EVENT_AGGREGATE
								.getColumn()) == 0) {

					if (null != column.getValue()) {

						userSummary.setEventTypeAggregate(MapType.getInstance(
								UTF8Type.instance, DoubleType.instance).compose(
								column.getValue()));
					}

				}

			}
		}
		return userSummary;
	}

}
