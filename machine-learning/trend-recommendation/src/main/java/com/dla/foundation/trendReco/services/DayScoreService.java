package com.dla.foundation.trendReco.services;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.dla.foundation.trendReco.model.DayScore;
import com.dla.foundation.trendReco.model.UserSummary;

/**
 * This class provides the functionality of summarizing day score on the item
 * basis of similar date,tenant,region.
 * 
 * @author shishir_shivhare
 * @version: 1.0
 * @since 2014-06-18
 */
public class DayScoreService implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7989855742174025415L;

	public static final String DELIMITER_PROPERTY = "#";
	private static final Logger logger = Logger
			.getLogger(DayScoreService.class);

	/**
	 * 
	 * @param userEventRDD
	 *            : It requires RDD with keys as string and UserSummary as
	 *            value. keys is trendid#regionid#itemid#date
	 * @return Day score
	 * 
	 */

	public JavaRDD<DayScore> calculateScore(
			JavaPairRDD<String, UserSummary> userSummaryRDD) {
		logger.info("Day Score Service: Grouping records on the basis of date per item,tenant,region");
		JavaPairRDD<String, Iterable<UserSummary>> UserSummaryGroupedByItemRDD = userSummaryRDD
				.distinct().groupByKey();

		logger.info("Day Score Service: Combining All events of item of similar date,tenant,region into single record and computing its day score");
		JavaRDD<DayScore> dayScorePerItemRDD = UserSummaryGroupedByItemRDD
				.map(new Function<Tuple2<String, Iterable<UserSummary>>, DayScore>() {

					/**
							 * 
							 */
					private static final long serialVersionUID = -7224382762060322982L;
					Iterator<UserSummary> userSummaryIterator;
					UserSummary userSummary;
					DayScore dayScore;
					Map<String, Integer> eventAggregate;

					@Override
					public DayScore call(
							Tuple2<String, Iterable<UserSummary>> records)
							throws Exception {

						int count = 0;
						double dayScoreCount = 0;
						String[] keys = records._1.split(DELIMITER_PROPERTY);
						eventAggregate = new HashMap<String, Integer>();
						userSummaryIterator = records._2.iterator();
						while (userSummaryIterator.hasNext()) {
							userSummary = userSummaryIterator.next();
							for (Entry<String, Integer> event : userSummary
									.getEventTypeAggregate().entrySet()) {
								if (eventAggregate.containsKey(event.getKey())) {
									count = eventAggregate.get(event.getKey());
									count += event.getValue();
									eventAggregate.put(event.getKey(), count);

								} else {
									eventAggregate.put(event.getKey(),
											event.getValue());
								}
							}
							dayScoreCount += userSummary.getDayScore();

						}
						dayScore = new DayScore(keys[0],keys[1], Long.parseLong(keys[3]),keys[2], eventAggregate,dayScoreCount);
						return dayScore;
					}
				});
		return dayScorePerItemRDD;

	}

}