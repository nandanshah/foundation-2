package com.dla.foundation.trendReco.services;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.map.HashedMap;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.trendReco.model.EventType;
import com.dla.foundation.trendReco.model.UserEvent;
import com.dla.foundation.trendReco.model.UserSummary;

/**
 * This class provides the functionality of computing day score per user.
 * Records are distinguished by item ,date ,tenant,region. So the key here is :
 * trendid#regionid#itemid#date#user#eventtype
 * 
 * @author shishir_shivhare
 * 
 */
public class UserEvtSummaryService implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5095480949340422140L;
	private static final String COUNT = "count";
	private static final String WEIGHT = "weight";
	private static final String VALUE = "value";

	private Map<String, EventType> requiredEvent;
	private Iterator<UserEvent> userEventIterator;
	private UserEvent userEvent;
	public static final String DELIMITER_PROPERTY = "#";
	private static final Logger logger = Logger
			.getLogger(UserEvtSummaryService.class);

	public Map<String, EventType> getRequiredEvent() {
		return requiredEvent;
	}

	public UserEvtSummaryService(Map<String, EventType> requiredevent) {
		// Map which will contain event id as key and Event type as value. It is
		// used to check whether event is required or not and to get threshold
		// of event if required.
		this.requiredEvent = requiredevent;

	}

	/**
	 * 
	 * @param userEventRDD
	 *            : It requires RDD with keys as string and userEvent. keys is
	 *            trendid#regionid#itemid#date#user#eventtype
	 * @return Summary of the user with day score for the events he fired i.e
	 *         what all events he fired for the item in particular date based on
	 *         tenant,region,item.
	 */

	public JavaRDD<UserSummary> calculateUserSummary(
			JavaPairRDD<String, UserEvent> userEventRDD) {

		logger.info("User Event Summary Service: Grouping records on the basis of user");
		// It will group by key which is stated as
		// trendid#regionid#itemid#date#user#eventtype
		JavaPairRDD<String, Iterable<UserEvent>> groupedUserEventRDD = userEventRDD
				.distinct().groupByKey();

		// groupedUserEventRDDByEvent will have key as:
		// trendid#regionid#itemid#date#user
		logger.info("User Event Summary Service: Calculating daily count on the basis of user");
		JavaPairRDD<String, Map<String, Map<String, Double>>> groupedUserEventRDDByEvent = calculateDailyEventCountbyUser(groupedUserEventRDD);

		// It will group by key which is trendid#regionid#itemid#date#user.
		// Below
		// function calculates event count by date & Item. It will return
		// similar key
		// with collection of map
		// which contains each map object with event id and its count.
		logger.info("User Event Summary Service: Grouping the items with all  the events");
		JavaPairRDD<String, Iterable<Map<String, Map<String, Double>>>> groupedEventCountRDD = groupedUserEventRDDByEvent
				.distinct().groupByKey();

		logger.info("User Event Summary Service: Combining all the events of item with its count into single record");
		JavaRDD<UserSummary> userSummaryRDD = combineAllEventOfRecord(groupedEventCountRDD);

		return userSummaryRDD;

	}

	/**
	 * This function will combine all maps(having event id and its count) in the
	 * value into single map named as event aggregate map and also calculate day
	 * score for the user depending upon the weight specified for each event
	 * present in required event map(it is initialized in constructor of User
	 * Event Summary service).
	 * 
	 * @param groupedEventCountByDateRDD
	 *            : It will java pair RDD with key as
	 *            trendid#regionid#itemid#date#user and value as collection of
	 *            map which contains each map object with event id and it count.
	 * @return User Event Summary.
	 */

	private JavaRDD<UserSummary> combineAllEventOfRecord(
			JavaPairRDD<String, Iterable<Map<String, Map<String, Double>>>> groupedEventCountByDateRDD) {

		JavaRDD<UserSummary> dayScoreRDD = groupedEventCountByDateRDD
				.map(new Function<Tuple2<String, Iterable<Map<String, Map<String, Double>>>>, UserSummary>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 4230933280991379283L;
					Iterator<Map<String, Map<String, Double>>> eventsWithCount;
					EventType eventType;
					UserSummary userSummary;

					Map<String, Double> eventAggregate;

					public UserSummary call(
							Tuple2<String, Iterable<Map<String, Map<String, Double>>>> record)
							throws Exception {

						eventAggregate = new HashMap<>();
						double dailyScore = 0;
						eventsWithCount = record._2.iterator();
						while (eventsWithCount.hasNext()) {
							for (Entry<String, Map<String, Double>> eventWithCount : eventsWithCount
									.next().entrySet()) {
								eventAggregate.put(eventWithCount.getKey(),
										eventWithCount.getValue().get(COUNT));

								dailyScore += eventWithCount.getValue().get(
										WEIGHT);

							}
						}
						String[] keys = record._1.split(DELIMITER_PROPERTY);
						userSummary = new UserSummary(keys[0], keys[1],
								keys[2], Long.parseLong(keys[3]), keys[4],
								eventAggregate, dailyScore);
						return userSummary;
					}
				});
		return dayScoreRDD;
	}

	/**
	 * This function calculate event count by user.It requires for checking
	 * progressive events like watch. In Event like watch it has multiple
	 * records shown as below: 1. insert into
	 * usereventsummarytable(id,tenantid,regionid,userid, eventtype,
	 * movieid,timestamp,avp,flag,date) values (
	 * 0BA3ECD3-51A8-4B52-BDC0-00C628649B4D,1,1,1,3,122,1404111596,{'watchperc':'0'},1,'2014-06-30'
	 * ) ; 2. insert into usereventsummarytable(id,tenantid,regionid,userid,
	 * eventtype, movieid,timestamp,avp,flag,date) values (
	 * 8EB93354-1B9E-4BB9-B87A-E113D02986F5,1,1,1,3,122,1404111596,{'watchperc':'20'},1,'2014-06-30'
	 * ) ;
	 * 
	 * Same as above records it will have progress till 100 if watch completes.
	 * We need to emit count as 1 only when threshold(in watch consider
	 * watchperc) > threshold specified by user( requiredEvent map is
	 * initialized while creating day score object). If same user likes same
	 * item multiple times in a day it will count them. But if same user watch
	 * same video (eg:matrix) multiple times in a day then it will consider it
	 * as 1(need to implement logic for it if required).
	 * 
	 * @param groupedUserEventRDDByUser
	 *            : it will take java pair RDD grouped by
	 *            key(trendid#regionid#itemid#date#user#eventtype)
	 * @return after calculating score by user for each event it will remove
	 *         user id from key and emit key(trendid#regionid#itemid#date#user)
	 *         with its count.
	 */
	private JavaPairRDD<String, Map<String, Map<String, Double>>> calculateDailyEventCountbyUser(
			JavaPairRDD<String, Iterable<UserEvent>> groupedUserEventRDDByUser) {
		JavaPairRDD<String, Map<String, Map<String, Double>>> groupedUserEventRDDByEvent = groupedUserEventRDDByUser
				.mapToPair(new PairFunction<Tuple2<String, Iterable<UserEvent>>, String, Map<String, Map<String, Double>>>() {

					/**
			 * 
			 */
					private static final long serialVersionUID = 3747974274073364215L;
					EventType recordEventType;
					String primaryKey;
					Map<String, Map<String, Double>> eventAggregate;

					@SuppressWarnings("unchecked")
					public Tuple2<String, Map<String, Map<String, Double>>> call(
							Tuple2<String, Iterable<UserEvent>> records)
							throws Exception {
						Map<String, Double> count;
						boolean thresholdFlag = false;
						eventAggregate = new HashedMap();
						String[] keys = records._1.split(DELIMITER_PROPERTY);

						recordEventType = requiredEvent.get(keys[5]);

						thresholdFlag = checkForThreshold(recordEventType);
						if (thresholdFlag) {
							count = processingDataWithThreshold(records,
									recordEventType);
						} else {
							count = processingDataWithoutThreshold(records,
									recordEventType);

						}
						primaryKey = keys[0] + DELIMITER_PROPERTY + keys[1]
								+ DELIMITER_PROPERTY + keys[2]
								+ DELIMITER_PROPERTY + keys[3]
								+ DELIMITER_PROPERTY + keys[4];
						eventAggregate.put(keys[5], count);
						return new Tuple2<String, Map<String, Map<String, Double>>>(
								primaryKey, eventAggregate);

					}
				});
		return groupedUserEventRDDByEvent;
	}

	/**
	 * This function will provide the logic to deal with progressive events. It
	 * calculate the count depending upon the record crosses the threshold or
	 * not.
	 * 
	 */
	private Map<String, Double> processingDataWithThreshold(
			Tuple2<String, Iterable<UserEvent>> records,
			EventType eventTypeWithThreshold) {
		double thresholdValue = 0;
		Map<String, Double> countWeight = new HashedMap();
		UserEvent maxThresholdUserEvent = null;
		userEventIterator = records._2.iterator();

		thresholdValue = eventTypeWithThreshold.getThreshold();

		while (userEventIterator.hasNext()) {
			userEvent = userEventIterator.next();

			if (-1 != userEvent.getPlayPercentage()) {
				maxThresholdUserEvent = userEvent;
				break;
			}
		}
		while (userEventIterator.hasNext()) {
			userEvent = userEventIterator.next();

			if (-1 != userEvent.getPlayPercentage()) {

				if (maxThresholdUserEvent.getPlayPercentage() < userEvent
						.getPlayPercentage()) {
					maxThresholdUserEvent = userEvent;
				}
			}
		}

		if (null != maxThresholdUserEvent) {
			if (thresholdValue < maxThresholdUserEvent.getPlayPercentage()) {
				countWeight.put(COUNT, 1.0);
				countWeight.put(WEIGHT, (1 * eventTypeWithThreshold.getWeight()
						.get(VALUE)) / 100);
			}
		}

		return countWeight;
	}

	/**
	 * This function will provide the logic to deal with non progressive events.
	 * 
	 */
	private Map<String, Double> processingDataWithoutThreshold(
			Tuple2<String, Iterable<UserEvent>> records, EventType eventType) {

		Map<String, Double> countWeight = new HashedMap();
		int rateScore = 0;
		boolean rateEventFlag = false;
		userEventIterator = records._2.iterator();

		while (userEventIterator.hasNext()) {

			UserEvent userEvent = userEventIterator.next();

			if (userEvent.getRatescore() >= 0) {

				rateEventFlag = true;
				rateScore = (int) userEvent.getRatescore();
			} else {
				break;
			}
		}

		if (rateEventFlag == false) {

			countWeight.put(COUNT, 1.0);
			countWeight.put(WEIGHT,
					(1 * eventType.getWeight().get(VALUE)) / 100);

		} else {

			countWeight.put(COUNT, 1.0);
			countWeight
					.put(WEIGHT,
							(1 * eventType.getWeight().get(
									String.valueOf(rateScore)) / 100));
		}

		return countWeight;
	}

	private boolean checkForThreshold(EventType event) {
		if (null != event && -1 != event.getThreshold()) {
			return true;
		} else {
			return false;
		}
	}

}
