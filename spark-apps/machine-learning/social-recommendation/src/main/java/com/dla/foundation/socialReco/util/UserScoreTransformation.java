package com.dla.foundation.socialReco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.socialReco.model.DailyEventSummaryPerItem;
import com.dla.foundation.socialReco.model.DayScore;

/**
 * This class provides the utility to convert the record (format provided
 * through spark-cassandra connector) to Day Score.
 * 
 * @author prajakta_bhosale
 * 
 */
public class UserScoreTransformation implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String DELIMITER_PROPERTY = "#";

	public static JavaPairRDD<String, DayScore> dataTransformation(
			JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {


		JavaPairRDD<String, DayScore> userEventRDD = cassandraRDD
				.mapToPair(new PairFunction<Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>>, String, DayScore>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -5603807529160303375L;
					DayScore dayScore = null;

					public Tuple2<String, DayScore> call(
							Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record)
									throws Exception {

						dayScore = getDayScore(record);
						String primaryKey = dayScore.getTenantId()
								+ DELIMITER_PROPERTY + dayScore.getRegionId()
								+ DELIMITER_PROPERTY + dayScore.getItemId()
								+ DELIMITER_PROPERTY + dayScore.getUserId();

						return new Tuple2<String, DayScore>(primaryKey,
								dayScore);

					}
				});

		return userEventRDD;
	}

	private static com.dla.foundation.socialReco.model.DayScore getDayScore(
			Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record) {

		com.dla.foundation.socialReco.model.DayScore dayScore = new DayScore();
		TimestampType timestampType = TimestampType.instance;
		Map<String, ByteBuffer> priamryKeyColumns = record._1();
		if (priamryKeyColumns != null) {
			for (Entry<String, ByteBuffer> column : priamryKeyColumns
					.entrySet()) {

				if (column.getKey().toLowerCase()
						.compareTo(DailyEventSummaryPerItem.TENANT.getColumn()) == 0) {
					if (null != column.getValue())
						dayScore.setTenantId(UUIDType.instance.compose(column
								.getValue()).toString());

				} else if (column.getKey().toLowerCase()
						.compareTo(DailyEventSummaryPerItem.REGION.getColumn()) == 0) {
					if (null != column.getValue())
						dayScore.setRegionId(UUIDType.instance.compose(column
								.getValue()).toString());

				} else if (column.getKey().toLowerCase()
						.compareTo(DailyEventSummaryPerItem.ITEM.getColumn()) == 0) {
					if (null != column.getValue())
						dayScore.setItemId(UUIDType.instance.compose(column
								.getValue()).toString());

				}else if (column.getKey().toLowerCase()
						.compareTo(DailyEventSummaryPerItem.USER.getColumn()) == 0) {
					if (null != column.getValue())
						dayScore.setUserId(UUIDType.instance.compose(column
								.getValue()).toString());

				}

			}
		}
		Map<String, ByteBuffer> otherColumns = record._2;
		if (otherColumns != null) {

			for (Entry<String, ByteBuffer> column : otherColumns.entrySet()) {
				if (column
						.getKey()
						.toLowerCase()
						.compareTo(
								DailyEventSummaryPerItem.DAY_SCORE.getColumn()) == 0) {
					if (null != column.getValue())
						dayScore.setDayScore(ByteBufferUtil.toDouble(column
								.getValue()));
				} else if (column.getKey().toLowerCase()
						.compareTo(DailyEventSummaryPerItem.DATE.getColumn()) == 0) {
					if (null != column.getValue())
						dayScore.setDate(timestampType.compose(column.getValue()));
				}else if (column.getKey().compareToIgnoreCase(
						DailyEventSummaryPerItem.EVENT_AGGREGATE
						.getColumn()) == 0) {
						if (null != column.getValue()) {
							dayScore.setEventTypeAggregate(MapType.getInstance(
						UTF8Type.instance, DoubleType.instance).compose(
						column.getValue()));
						}
				}
			}
		}
		return dayScore;
	}
}
