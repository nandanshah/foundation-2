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
		JavaPairRDD<String, Map<String, Integer>> groupedUserEventRDDByEvent = calculateDailyEventCountbyUser(groupedUserEventRDD);
		
		// It will group by key which is trendid#regionid#itemid#date#user.
		// Below
		// function calculates event count by date & Item. It will return
		// similar key
		// with collection of map
		// which contains each map object with event id and its count.
		logger.info("User Event Summary Service: Grouping the items with all  the events");
		JavaPairRDD<String, Iterable<Map<String, Integer>>> groupedEventCountRDD = groupedUserEventRDDByEvent
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
			JavaPairRDD<String, Iterable<Map<String, Integer>>> groupedEventCountByDateRDD) {

		JavaRDD<UserSummary> dayScoreRDD = groupedEventCountByDateRDD
				.map(new Function<Tuple2<String, Iterable<Map<String, Integer>>>, UserSummary>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 4230933280991379283L;
					Iterator<Map<String, Integer>> eventsWithCount;
					EventType eventType;
					UserSummary userSummary;

					Map<String, Integer> eventAggregate;
					public UserSummary call(
							Tuple2<String, Iterable<Map<String, Integer>>> record)
							throws Exception {

						eventAggregate = new HashMap<>();
						double dailyScore = 0;
						eventsWithCount = record._2.iterator();
						while (eventsWithCount.hasNext()) {
							for (Entry<String, Integer> eventWithCount : eventsWithCount
									.next().entrySet()) {
								eventAggregate.put(eventWithCount.getKey(),
										eventWithCount.getValue());
								eventType = requiredEvent.get(eventWithCount
										.getKey());

								if (null != eventType) {
									// calculation of day score based on the
									// weight specified for each event.
									dailyScore += (((eventType.getWeight() / 100.0) * eventWithCount
											.getValue()));
								}
							}
						}
						String[] keys = record._1.split(DELIMITER_PROPERTY);
						userSummary = new UserSummary(
								keys[0], keys[1], keys[2], Long
										.parseLong(keys[3]),keys[4], eventAggregate,
								dailyScore);
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
	 * 0BA3ECD3-51A8-4B52-BDC0-00C628649B4D,1,1,1,3,122,1404111596,{'watchperc':'0'},1,'2014-06-30')
	 * ; 2. insert into usereventsummarytable(id,tenantid,regionid,userid,
	 * eventtype, movieid,timestamp,avp,flag,date) values (
	 * 8EB93354-1B9E-4BB9-B87A-E113D02986F5,1,1,1,3,122,1404111596,{'watchperc':'20'},1,'2014-06-30')
	 * ;
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
	private JavaPairRDD<String, Map<String, Integer>> calculateDailyEventCountbyUser(
			JavaPairRDD<String, Iterable<UserEvent>> groupedUserEventRDDByUser) {
		JavaPairRDD<String, Map<String, Integer>> groupedUserEventRDDByEvent = groupedUserEventRDDByUser
				.mapToPair(new PairFunction<Tuple2<String, Iterable<UserEvent>>, String, Map<String, Integer>>() {

					/**
			 * 
			 */
					private static final long serialVersionUID = 3747974274073364215L;
					EventType recordEventType;
					String primaryKey;
					Map<String, Integer> eventAggregate;

					@SuppressWarnings("unchecked")
					public Tuple2<String, Map<String, Integer>> call(
							Tuple2<String, Iterable<UserEvent>> records)
							throws Exception {
						int count = 0;
						boolean thresholdFlag = false;
						eventAggregate = new HashedMap();
						String[] keys = records._1.split(DELIMITER_PROPERTY);

						recordEventType = requiredEvent.get(keys[5]);
						
						thresholdFlag = checkForThreshold(recordEventType);
						if (thresholdFlag) {
							count = processingDataWithThreshold(records,
									recordEventType);
						} else {
							count = processingDataWithoutThreshold(records);

						}
						primaryKey = keys[0] + DELIMITER_PROPERTY + keys[1]
								+ DELIMITER_PROPERTY + keys[2]
								+ DELIMITER_PROPERTY + keys[3]
								+ DELIMITER_PROPERTY + keys[4];
						eventAggregate.put(keys[5], count);
						return new Tuple2<String, Map<String, Integer>>(
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
	private int processingDataWithThreshold(
			Tuple2<String, Iterable<UserEvent>> records,
			EventType eventTypeWithThreshold) {
		String thresholdKey = null;
		int thresholdValue = 0;
		int count = 0;
		UserEvent maxThresholdUserEvent = null;
		userEventIterator = records._2.iterator();

		for (Entry<String, Integer> threshold : eventTypeWithThreshold
				.getThreshold().entrySet()) {
			thresholdKey = threshold.getKey();
			thresholdValue = threshold.getValue();
		}
		while (userEventIterator.hasNext()) {
			userEvent = userEventIterator.next();
			if (null != userEvent.getAvp()) {
				if (userEvent.getAvp().containsKey(thresholdKey)) {
					maxThresholdUserEvent = userEvent;
					break;
				}
			}
		}
		while (userEventIterator.hasNext()) {
			userEvent = userEventIterator.next();
			if (null != userEvent.getAvp()) {
				if (userEvent.getAvp().containsKey(thresholdKey)) {
					if (Integer.parseInt(maxThresholdUserEvent.getAvp().get(
							thresholdKey)) < Integer.parseInt(userEvent
							.getAvp().get(thresholdKey))) {
						maxThresholdUserEvent = userEvent;
					}
				}
			}
		}
		if (null != maxThresholdUserEvent) {
			if (thresholdValue < Integer.parseInt(maxThresholdUserEvent
					.getAvp().get(thresholdKey))) {
				count = 1;
			}
		}
		return count;
	}

	/**
	 * This function will provide the logic to deal with non progressive events.
	 * 
	 */
	private int processingDataWithoutThreshold(
			Tuple2<String, Iterable<UserEvent>> records) {
		int count = 0;

		userEventIterator = records._2.iterator();

		while (userEventIterator.hasNext()) {
			userEventIterator.next();
			count += 1;
		}
		return count;
	}

	private boolean checkForThreshold(EventType event) {
		if (null != event && null != event.getThreshold()) {
			return true;
		} else {
			return false;
		}
	}

}
