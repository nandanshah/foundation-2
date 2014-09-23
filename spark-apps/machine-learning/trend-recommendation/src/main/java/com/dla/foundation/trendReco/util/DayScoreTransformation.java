package com.dla.foundation.trendReco.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.trendReco.model.DailyEventSummaryPerItem;
import com.dla.foundation.trendReco.model.DailyEventSummaryPerUserItem;
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
		Map<String, ByteBuffer> priamryKeyColumns = record._1();
		if (priamryKeyColumns != null) {

			if (priamryKeyColumns.get(DailyEventSummaryPerItem.TENANT
					.getColumn().toLowerCase()) != null
					&& priamryKeyColumns.get(DailyEventSummaryPerItem.REGION
							.getColumn().toLowerCase()) != null
					&& priamryKeyColumns.get(DailyEventSummaryPerItem.ITEM
							.getColumn().toLowerCase()) != null) {

				dayScore.setTenantId(UUIDType.instance.compose(
						priamryKeyColumns.get(DailyEventSummaryPerItem.TENANT
								.getColumn().toLowerCase())).toString());
				dayScore.setRegionId(UUIDType.instance.compose(
						priamryKeyColumns.get(DailyEventSummaryPerItem.REGION
								.getColumn().toLowerCase())).toString());
				dayScore.setItemId(UUIDType.instance.compose(
						priamryKeyColumns.get(DailyEventSummaryPerItem.ITEM
								.getColumn().toLowerCase())).toString());

			} else {
				return null;
			}
		}

		Map<String, ByteBuffer> otherColumns = record._2;

		if (otherColumns != null) {

			if (otherColumns.get(DailyEventSummaryPerUserItem.DAY_SCORE
					.getColumn().toLowerCase()) != null
					&& otherColumns.get(DailyEventSummaryPerUserItem.DATE
							.getColumn().toLowerCase()) != null) {

				dayScore.setDayScore(ByteBufferUtil.toDouble(otherColumns
						.get(DailyEventSummaryPerItem.DAY_SCORE.getColumn()
								.toLowerCase())));

				dayScore.setTimestamp(TrendRecommendationUtil
						.getFormattedDate(TimestampType.instance.compose(
								otherColumns.get(DailyEventSummaryPerItem.DATE
										.getColumn().toLowerCase())).getTime()));

			}
		}
		return dayScore;
	}
}