package com.dla.foundation.trendReco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.trendReco.model.DailyEventSummaryPerItem;
import com.dla.foundation.trendReco.model.DayScore;

/**
 * This class provides the utility to convert the record (format provided
 * through spark-cassandra connector) to Day Score.
 * 
 * @author shishir_shivhare
 * 
 */
public class DayScoreTransformation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8402725530068887012L;
	private static final String DELIMITER_PROPERTY = "#";

	public static JavaPairRDD<String, DayScore> getDayScoreWithKey(
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
								+ DELIMITER_PROPERTY + dayScore.getItemId();

						return new Tuple2<String, DayScore>(primaryKey,
								dayScore);
					}
				});

		return userEventRDD;
	}

	private static DayScore getDayScore(
			Tuple2<Map<String, ByteBuffer>, Map<String, ByteBuffer>> record) {

		DayScore dayScore = new DayScore();
		TimestampType timestampType = TimestampType.instance;
		Map<String, ByteBuffer> priamryKeyColumns = record._1();
		if (priamryKeyColumns != null) {
			for (Entry<String, ByteBuffer> column : priamryKeyColumns
					.entrySet()) {

				if (column.getKey()
						.compareToIgnoreCase(DailyEventSummaryPerItem.TENANT.getColumn()) == 0) {
					if (null != column.getValue())
						dayScore.setTenantId(UUIDType.instance.compose(column
								.getValue()).toString());

				} else if (column.getKey()
						.compareToIgnoreCase(DailyEventSummaryPerItem.REGION.getColumn()) == 0) {
					if (null != column.getValue())
						dayScore.setRegionId(UUIDType.instance.compose(column
								.getValue()).toString());

				} else if (column.getKey()
						.compareToIgnoreCase(DailyEventSummaryPerItem.ITEM.getColumn()) == 0) {
					if (null != column.getValue())
						dayScore.setItemId(UUIDType.instance.compose(column
								.getValue()).toString());

				}

			}
		}
		Map<String, ByteBuffer> otherColumns = record._2;
		if (otherColumns != null) {

			for (Entry<String, ByteBuffer> column : otherColumns.entrySet()) {
				if (column
						.getKey()
						.compareToIgnoreCase(DailyEventSummaryPerItem.DAY_SCORE.getColumn()) == 0) {
					if (null != column.getValue())
						dayScore.setDayScore(ByteBufferUtil.toDouble(column
								.getValue()));
				} else if (column.getKey()
						.compareToIgnoreCase(DailyEventSummaryPerItem.DATE.getColumn()) == 0) {
					if (null != column.getValue())
						dayScore.setTimestamp(timestampType.compose(
								column.getValue()).getTime());
				}
			}
		}
		return dayScore;
	}
}
