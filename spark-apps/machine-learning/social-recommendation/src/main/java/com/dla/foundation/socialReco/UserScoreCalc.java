package com.dla.foundation.socialReco;


import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.socialReco.model.DayScore;
import com.dla.foundation.socialReco.model.UserScore;
import com.dla.foundation.socialReco.util.PropKeys;
import com.dla.foundation.socialReco.util.SocialRecommendationUtil;


public class UserScoreCalc implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(UserScoreCalc.class);
	private static final String DELIMITER_PROPERTY = "#";
	private static Iterator<UserScore> userScoreIterator;
	private static Iterator<DayScore> dayScoreIterator;
	
	public static JavaPairRDD<String, UserScore> userScoreCalculation(JavaPairRDD<String, DayScore> readableFormatRDD,
			Date formattedYday,
			Date formattedWeek, final PropertiesHandler socialScoreProp 
			) throws IOException, ParseException {
	
		//We can't access properties handles inside the nested functions ...so calculating the values here & passing it to function
		final Double freshness_bucket_for_last_day = Double.parseDouble(socialScoreProp.getValue(
				PropKeys.FRESHNESS_BUCKET_FOR_LAST_DAY.getValue()));
		final Double freshness_bucket_for_last_week = Double.parseDouble(socialScoreProp.getValue(
				PropKeys.FRESHNESS_BUCKET_FOR_LAST_WEEK.getValue()));
		final Double freshness_bucket_for_last_month = Double.parseDouble(socialScoreProp.getValue(
				PropKeys.FRESHNESS_BUCKET_FOR_LAST_MONTH.getValue()));
		
		logger.info("Score Calculation based on Dates"); 
		JavaPairRDD<String, DayScore> scoreBasedOnDatesRDD = calculateScoreBasedOnDates
				(readableFormatRDD, formattedYday, formattedWeek,  
						freshness_bucket_for_last_day, freshness_bucket_for_last_week, freshness_bucket_for_last_month);
		
		logger.info(" ************** Grouping based on user and movie ************** ");
		JavaPairRDD<String, Iterable<DayScore>> userMovieGroupedRDD = scoreBasedOnDatesRDD.distinct().groupByKey();
				
		// Score calculations based on per user movie
		logger.info(" ************** Score calculations based on per user movie ************** ");
		JavaPairRDD<String, UserScore> userMovieScoreRDD = calculateUserMovieScore(userMovieGroupedRDD);
			
		// Grouping based on User
		logger.info(" ************** Grouping based on User ************** ");
		JavaPairRDD<String, Iterable<UserScore>> userGroupedRDD = userMovieScoreRDD.distinct().groupByKey();
		
		// Score calculations per user
		logger.info(" ************** Score calculations per user ************** ");
		JavaPairRDD< String, UserScore> userScoreRDD = calculateUserScore(userGroupedRDD);
		
		return userScoreRDD;
	
	}
	
	/**
	 * 
	 * Score calculations based on Dates and the weight(present in cassandra table)
	 * 
	 * @param dayScoreRDD
	 * @param formattedYday
	 * @param formattedWeek
	 * @param freshness_bucket_for_last_day
	 * @param freshness_bucket_for_last_week
	 * @param freshness_bucket_for_last_month
	 * @return
	 */
	public static JavaPairRDD<String, DayScore> calculateScoreBasedOnDates(JavaPairRDD<String, DayScore> dayScoreRDD,
			final Date formattedYday,final Date formattedWeek, 
			final Double freshness_bucket_for_last_day,final Double freshness_bucket_for_last_week,
			final Double freshness_bucket_for_last_month){
		
		long endTimestamp = SocialRecommendationUtil
				.getFormattedDate(formattedWeek.getTime());
		
		logger.info("Freshness Buccket values : "+ freshness_bucket_for_last_day +" , "+ freshness_bucket_for_last_week +" , "+ freshness_bucket_for_last_month);
		JavaPairRDD<String, DayScore> userEventRDDWithScore = dayScoreRDD
				.mapToPair(new PairFunction<Tuple2<String, DayScore>, String, DayScore>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Tuple2<String, DayScore> call(
							Tuple2<String, DayScore> records)
									throws Exception {
						
						String primaryKey = records._1;
						DayScore dayScore = records._2;
						if(dayScore.getDate().compareTo(formattedYday) == 0)
						{
							dayScore.setDayScore(dayScore.getDayScore() * freshness_bucket_for_last_day );
							
						}else if(dayScore.getDate().before(formattedYday) && dayScore.getDate().after(formattedWeek) )
						{
							dayScore.setDayScore(dayScore.getDayScore() * freshness_bucket_for_last_week );
						}else
						{
							dayScore.setDayScore(dayScore.getDayScore() * freshness_bucket_for_last_month );
						}
						return new Tuple2<String, DayScore>(primaryKey,	dayScore);
					}

				});
		
		return userEventRDDWithScore;

	}
	
	/**
	 * 
	 * This method calculate the user score for list of movies present against that user.
	 * Example : User U1 watch + Like  the movie M1 ... then add the score of both and 
	 * Assign the reason.
	 *  
	 * @param dayScoreRDD2
	 * @return
	 */

	public static JavaPairRDD<String, UserScore> calculateUserMovieScore(JavaPairRDD<String, Iterable<DayScore>> dayScoreRDD2)
	{
		
		JavaPairRDD<String, UserScore> groupedUserEventRDDByEvent = dayScoreRDD2
				.mapToPair(new PairFunction<Tuple2<String, Iterable<DayScore>>, String, UserScore>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;
					String primaryKey;
					Map<String, Double> movieScore = null;
					Map<String, Double>eventTypeAggregate = null;
					Map<String, Map<String, Double>>finalEventTypeAggregate = null;
					Double score = 0.0;
					UserScore userScore = null;
					
					
					public Tuple2<String, UserScore> call(
							Tuple2<String, Iterable<DayScore>> records)
									throws Exception {
						
						movieScore = new HashMap<String, Double>();
						eventTypeAggregate = new HashMap<String, Double>();
						finalEventTypeAggregate = new HashMap<String, Map<String,Double>>();
						score = 0.0;
						userScore = null;
						
						String[] keys = records._1.split(DELIMITER_PROPERTY);
						primaryKey = keys[0] + DELIMITER_PROPERTY + keys[1]
								+ DELIMITER_PROPERTY + keys[3];

						//DayScore dayScore =new DayScore();
						dayScoreIterator = records._2.iterator();
						DayScore dayScore = new DayScore();
						while (dayScoreIterator.hasNext()) {

							dayScore = dayScoreIterator.next();
							//Score Calculations ...
							score = score + dayScore.getDayScore();
							//Reason Calculations ....
							if(eventTypeAggregate.size() == 0){
								eventTypeAggregate = dayScore.getEventTypeAggregate();
							}else
							{
								Map<String, Double>temp;
								temp=dayScore.getEventTypeAggregate();
								for (String key : temp.keySet()) {
									if(eventTypeAggregate.containsKey(key))
									{
										Double count = eventTypeAggregate.get(key) + temp.get(key);
										eventTypeAggregate.put(key,count);
									}else {
										eventTypeAggregate.put(key, temp.get(key));
									}
								}


							}

						}
						movieScore.put(keys[2], score);
						finalEventTypeAggregate.put(keys[2], eventTypeAggregate);
						userScore = new UserScore(keys[0],keys[1],keys[3],movieScore,finalEventTypeAggregate);

						return new Tuple2<String, UserScore>(primaryKey,
								userScore);
					}
				});
		return groupedUserEventRDDByEvent;

	}
	
	/**
	 * 
	 * This method  calculate the score of user based on all the movies against that user
	 * 
	 * @param dayScoreRDDPerUser
	 * @return
	 * 
	 */

	public static JavaPairRDD<String,UserScore> calculateUserScore(JavaPairRDD<String, Iterable<UserScore>> dayScoreRDDPerUser)
	{
		JavaPairRDD<String, UserScore> groupedUserEventRDDByEvent = dayScoreRDDPerUser				
				.mapToPair(new PairFunction<Tuple2<String, Iterable<UserScore>>, String, UserScore>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;
					String primaryKey;
					UserScore finalUserScore = null;
					Map<String, Double> movieScore = new HashMap<String, Double>();
					Map<String, Map<String, Double>>finalEventTypeAggregate = new HashMap<String, Map<String,Double>>();

					public Tuple2<String, UserScore> call(
							Tuple2<String, Iterable<UserScore>> records)
									throws Exception {

						String[] keys = records._1.split(DELIMITER_PROPERTY);
						primaryKey = keys[2];
						Date date = null;
						userScoreIterator = records._2.iterator();
						UserScore userScore = new UserScore();
						while (userScoreIterator.hasNext()) {
							userScore = userScoreIterator.next();
							movieScore.putAll(userScore.getMovieScore());
							finalEventTypeAggregate.putAll(userScore.getEventTypeAggregate());
						}

						finalUserScore = new UserScore(keys[0],keys[1],keys[2],movieScore,finalEventTypeAggregate);

						return new Tuple2<String, UserScore>(primaryKey, finalUserScore);
					}
				});
		
		return groupedUserEventRDDByEvent;
		
	}
}
