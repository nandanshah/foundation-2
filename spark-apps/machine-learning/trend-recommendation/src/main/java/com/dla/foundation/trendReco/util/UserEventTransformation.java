package com.dla.foundation.trendReco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.trendReco.model.UserEvent;
import com.dla.foundation.trendReco.model.UserEventSummary;

/**
 * This class provides the utility to convert the record (format provided
 * through spark-cassandra connector) to userEvent.
 * 
 * @author shishir_shivhare
 * 
 */
public class UserEventTransformation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7622544742946263628L;
	private static final String DELIMITER_PROPERTY = "#";

	public static JavaPairRDD<String, UserEvent> getUserEventWithKey(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {

		JavaPairRDD<String, UserEvent> userEventRDD = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, UserEvent>() {
					/**
			 * 
			 */
					private static final long serialVersionUID = 5374973368305114892L;
					UserEvent userEvent = null;

					public Tuple2<String, UserEvent> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
							throws Exception {
						long date = -1;
						String primaryKey = "";
						userEvent = getUserEvent(record);
						if (null != userEvent) {

							date = userEvent.getDate().getTime();
							if (null != userEvent.getEventType()) {
								primaryKey = primaryKey.concat(userEvent
										.getTenantId()
										+ DELIMITER_PROPERTY
										+ userEvent.getRegionId()
										+ DELIMITER_PROPERTY
										+ userEvent.getItemid()
										+ DELIMITER_PROPERTY
										+ date
										+ DELIMITER_PROPERTY
										+ userEvent.getUserId()
										+ DELIMITER_PROPERTY
										+ userEvent.getEventType());
								if (0 != "".compareTo(primaryKey)) {

									return new Tuple2<String, UserEvent>(
											primaryKey, userEvent);
								}
							}

						}
						return null;

					}
				});

		return userEventRDD;
	}

	private static UserEvent getUserEvent(
			Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
			throws NumberFormatException, CharacterCodingException {

		UserEvent userEvent = new UserEvent();
		Map<String, ByteBuffer> priamryKeyColumns = record._1();
		if (priamryKeyColumns != null) {
			for (Entry<String, ByteBuffer> column : priamryKeyColumns
					.entrySet()) {

			}
		} else {
			return null;
		}

		Map<String, ByteBuffer> otherColumns = record._2;
		if (otherColumns != null) {

			if (otherColumns.get(UserEventSummary.TENANT.getColumn()
					.toLowerCase()) != null
					&& otherColumns.get(UserEventSummary.REGION.getColumn()
							.toLowerCase()) != null
					&& otherColumns.get(UserEventSummary.PROFILE.getColumn()
							.toLowerCase()) != null
					&& otherColumns.get(UserEventSummary.ITEM.getColumn()
							.toLowerCase()) != null
					&& otherColumns.get(UserEventSummary.EVENT_TYPE.getColumn()
							.toLowerCase()) != null
					&& otherColumns.get(UserEventSummary.TIMESTAMP.getColumn()
							.toLowerCase()) != null
					&& otherColumns.get(UserEventSummary.DATE.getColumn()
							.toLowerCase()) != null) {

				userEvent.setTenantId(UUIDType.instance.compose(
						otherColumns.get(UserEventSummary.TENANT.getColumn()
								.toLowerCase())).toString());
				userEvent.setRegionId(UUIDType.instance.compose(
						otherColumns.get(UserEventSummary.REGION.getColumn()
								.toLowerCase())).toString());
				userEvent.setUserId(UUIDType.instance.compose(
						otherColumns.get(UserEventSummary.PROFILE.getColumn()
								.toLowerCase())).toString());
				userEvent.setItemid(UUIDType.instance.compose(
						otherColumns.get(UserEventSummary.ITEM.getColumn()
								.toLowerCase())).toString());
				userEvent.setEventType(ByteBufferUtil.string(otherColumns
						.get(UserEventSummary.EVENT_TYPE.getColumn()
								.toLowerCase())));
				userEvent.setTimestamp(ByteBufferUtil.toLong(otherColumns
						.get(UserEventSummary.TIMESTAMP.getColumn()
								.toLowerCase())));
				userEvent.setDate(new Date(TrendRecommendationUtil
						.getFormattedDate(TimestampType.instance.compose(
								otherColumns.get(UserEventSummary.DATE
										.getColumn())).getTime())));
			} else {
				return null;
			}

			if (otherColumns.get(UserEventSummary.PLAY_PERCENTAGE.getColumn()
					.toLowerCase()) != null) {

				userEvent.setPlayPercentage(ByteBufferUtil
						.toDouble(otherColumns
								.get(UserEventSummary.PLAY_PERCENTAGE
										.getColumn().toLowerCase())));
			}

			if (otherColumns.get(UserEventSummary.RATE_SCORE.getColumn()
					.toLowerCase()) != null) {

				userEvent.setRatescore(ByteBufferUtil.toInt(otherColumns
						.get(UserEventSummary.RATE_SCORE.getColumn()
								.toLowerCase())));
			}
		} else {
			return null;
		}
		return userEvent;
	}
}