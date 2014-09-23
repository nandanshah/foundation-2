package com.dla.foundation.trendReco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Map;

import org.apache.cassandra.db.marshal.DoubleType;
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

			if (priamryKeyColumns.get(DailyEventSummaryPerUserItem.TENANT
					.getColumn().toLowerCase()) != null
					&& priamryKeyColumns
							.get(DailyEventSummaryPerUserItem.REGION
									.getColumn().toLowerCase()) != null
					&& priamryKeyColumns
							.get(DailyEventSummaryPerUserItem.PROFILE
									.getColumn().toLowerCase()) != null
					&& priamryKeyColumns.get(DailyEventSummaryPerUserItem.ITEM
							.getColumn().toLowerCase()) != null) {

				userSummary
						.setTenantId(UUIDType.instance
								.compose(
										priamryKeyColumns
												.get(DailyEventSummaryPerUserItem.TENANT
														.getColumn()
														.toLowerCase()))
								.toString());
				userSummary
						.setRegionId(UUIDType.instance
								.compose(
										priamryKeyColumns
												.get(DailyEventSummaryPerUserItem.REGION
														.getColumn()
														.toLowerCase()))
								.toString());
				userSummary
						.setUserId(UUIDType.instance
								.compose(
										priamryKeyColumns
												.get(DailyEventSummaryPerUserItem.PROFILE
														.getColumn()
														.toLowerCase()))
								.toString());
				userSummary.setItemId(UUIDType.instance.compose(
						priamryKeyColumns.get(DailyEventSummaryPerUserItem.ITEM
								.getColumn().toLowerCase())).toString());

			} else {
				return null;
			}
		} else {
			return null;
		}

		Map<String, ByteBuffer> otherColumns = record._2;

		if (otherColumns != null) {

			if (otherColumns.get(DailyEventSummaryPerUserItem.DAY_SCORE
					.getColumn().toLowerCase()) != null
					&& otherColumns.get(DailyEventSummaryPerUserItem.DATE
							.getColumn().toLowerCase()) != null
					&& otherColumns
							.get(DailyEventSummaryPerUserItem.EVENT_AGGREGATE
									.getColumn().toLowerCase()) != null) {

				userSummary.setDayScore(ByteBufferUtil.toDouble(otherColumns
						.get(DailyEventSummaryPerUserItem.DAY_SCORE.getColumn()
								.toLowerCase())));

				userSummary.setTimestamp(TrendRecommendationUtil
						.getFormattedDate(TimestampType.instance.compose(
								otherColumns
										.get(DailyEventSummaryPerUserItem.DATE
												.getColumn().toLowerCase()))
								.getTime()));

				userSummary
						.setEventTypeAggregate(MapType
								.getInstance(UTF8Type.instance,
										DoubleType.instance)
								.compose(
										otherColumns
												.get(DailyEventSummaryPerUserItem.EVENT_AGGREGATE
														.getColumn()
														.toLowerCase())));

			}
		}
		return userSummary;
	}

}