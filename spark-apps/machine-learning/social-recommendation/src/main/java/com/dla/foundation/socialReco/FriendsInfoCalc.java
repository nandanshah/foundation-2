package com.dla.foundation.socialReco;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.socialReco.model.UserScore;
import com.google.common.base.Optional;

import org.apache.log4j.Logger;

public class FriendsInfoCalc implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final String DELIMITER_PROPERTY = "#";
	private static Iterator<UserScore> userScoreIterator;
	private static Logger logger = Logger.getLogger(FriendsInfoCalc.class);


	public static JavaPairRDD<String, UserScore> scoreCalculations(JavaPairRDD<String, UserScore> userScore,
			JavaPairRDD<String, String> friendsInfo, String considerSelfReco)
			{
		//Left outer join ..as we want data for each friends
		JavaPairRDD<String, Tuple2<String, Optional<UserScore>>> joinedRDD= friendsInfo.leftOuterJoin(userScore);
		
		
		//Convert data into proper string & userScore format.
		JavaPairRDD<String, UserScore> requiredFormatRDD = getUserScore(joinedRDD);
		
		//Filter the null records
		JavaPairRDD<String, UserScore> notNullScoreRDD = filterItemSummary(requiredFormatRDD);
		
		//Get the distinct records
		JavaPairRDD<String, Iterable<UserScore>> dayScoreRDD2 = notNullScoreRDD.distinct().groupByKey();
		
		//Calculations based on per user movie
		JavaPairRDD<String, UserScore> dayScoreRDDPerUserMovie = calculateFinalUserMovieScore(dayScoreRDD2);
		
		// Check to consider selfReco or not 
		if(considerSelfReco.equals("1"))
		{
			logger.info("Check for SelfReco is true");
			//TODO check the i/p to dayScoreRDDPerUserMovie.join
			JavaPairRDD<String, UserScore> requiredFormat = convertPrimaryKey(userScore);
			
			//Join between dayScoreRDDPerUserMovie and userScore
		        JavaPairRDD<String, Tuple2<UserScore, Optional<UserScore>>> joinResult = dayScoreRDDPerUserMovie.leftOuterJoin(requiredFormat);	
			// Final user score considering the self recommendation
			JavaPairRDD<String, UserScore> dayScoreRDDPerUserMovie1 = calculateFinalUserMovieScoreByConsideringSelfReco(joinResult);
			dayScoreRDDPerUserMovie = dayScoreRDDPerUserMovie1;
		}
		
		return dayScoreRDDPerUserMovie;
	}

	/**
	 * 
	 * This method Convert data into proper string & userScore format.
	 * 
	 * @param lojresult
	 * @return
	 */
	private static JavaPairRDD<String, UserScore> getUserScore(
			JavaPairRDD<String, Tuple2<String, Optional<UserScore>>> lojresult) {
		JavaPairRDD<String, UserScore> userScoreRDD = lojresult
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<UserScore>>>, String, UserScore>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 7819779796058281951L;
					UserScore userScore, resultUserScore;
					String primaryKey ;
					public Tuple2<String, UserScore> call(
							Tuple2<String, Tuple2<String, Optional<UserScore>>> record)
									throws Exception {
						String userId = record._2._1;

						if (record._2._2.isPresent()) {
							userScore = record._2._2.get();
							primaryKey = userScore.getTenantId()
									+ DELIMITER_PROPERTY + userScore.getRegionId()
									+ DELIMITER_PROPERTY + userId;

							//resultUserScore.set
							resultUserScore = new UserScore(userScore.getTenantId(), userScore.getRegionId(), userId,
									userScore.getMovieScore(),userScore.getEventTypeAggregate());

							return new Tuple2<String, UserScore>(primaryKey, resultUserScore);

						}
						return null;	
					}
				});

		return userScoreRDD;
	}

	/**
	 * 
	 * This method filters the null records from the RDD
	 * 
	 * @param scoreRDD
	 * @return
	 */
	public static JavaPairRDD<String, UserScore> filterItemSummary(
			JavaPairRDD<String, UserScore> scoreRDD) {
		JavaPairRDD<String, UserScore> filteredScoreRDD = scoreRDD
				.filter(new Function<Tuple2<String, UserScore>, Boolean>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -6160930803403593402L;

					public Boolean call(Tuple2<String, UserScore> record)
							throws Exception {

						if (record != null) {
							if (null != record._1 && null != record._2) {
								return true;
							}
						}
						return false;
					}
				});

		return filteredScoreRDD;
	}

	public static JavaPairRDD<String, UserScore> calculateFinalUserMovieScore(JavaPairRDD<String, Iterable<UserScore>> dayScoreRDD2)
	{
		JavaPairRDD<String, UserScore> groupedUserEventRDDByEvent = dayScoreRDD2
				.mapToPair(new PairFunction<Tuple2<String, Iterable<UserScore>>, String, UserScore>() {
					private static final long serialVersionUID = 3747974274073364215L;
					String primaryKey;
					Map<String, Double> movieScore = new HashMap<String, Double>();
					Map<String, Map<String, Double>> eventTypeAggregate=new HashMap<String, Map<String, Double>>();
					UserScore finalUserScore = null;

					public Tuple2<String, UserScore> call(
							Tuple2<String, Iterable<UserScore>> records)
									throws Exception {
						String[] keys = records._1.split(DELIMITER_PROPERTY);

						primaryKey = keys[0] + DELIMITER_PROPERTY + keys[1]
								+ DELIMITER_PROPERTY + keys[2];
						// As want to join it with UserScore set the key as only userID
						//primaryKey = keys[2];
					
						//DayScore dayScore =new DayScore();
						userScoreIterator = records._2.iterator();
						UserScore userScore = new UserScore();
						while (userScoreIterator.hasNext()) {
							userScore = userScoreIterator.next();
							// Movie Score calculations : 
							//Reason Calculations ....
							if(eventTypeAggregate.size() == 0){
								movieScore = userScore.getMovieScore();
								eventTypeAggregate = userScore.getEventTypeAggregate();
							}else
							{
								for(String key : userScore.getMovieScore().keySet()){
									if(movieScore.containsKey(key))
									{
										Double score = movieScore.get(key) + userScore.getMovieScore().get(key);
										movieScore.put(key, score);
									}else
									{
										movieScore.put(key, userScore.getMovieScore().get(key));
									}
								}

								Map<String, Map<String, Double>>temp = userScore.getEventTypeAggregate();
								for (String key : temp.keySet()) {
									if(eventTypeAggregate.containsKey(key))
									{
										//	Map<String,Integer> temp2 = eventTypeAggregate.get(key);
										Map<String, Double> temp2 = temp.get(key);
										for (String key1 : temp2.keySet()) {
											if(eventTypeAggregate.get(key).containsKey(key1))
											{
												Double count = eventTypeAggregate.get(key).get(key1) + temp2.get(key1);
												eventTypeAggregate.get(key).put(key1,count);
											}
											else
											{
												eventTypeAggregate.get(key).put(key1, temp2.get(key1));
											}
										}
									}else{
										eventTypeAggregate.put(key, temp.get(key));
									}
								}
							}
						}

						finalUserScore = new UserScore(keys[0],keys[1],keys[2],movieScore,eventTypeAggregate);

						return new Tuple2<String, UserScore>(primaryKey,
								finalUserScore);
					}
				});
		return groupedUserEventRDDByEvent;
	}

	public static JavaPairRDD<String, UserScore> convertPrimaryKey(JavaPairRDD<String, UserScore> userScore){

		JavaPairRDD<String, UserScore> requiredFormatUserScoreRDD = userScore
				.mapToPair(new PairFunction<Tuple2<String, UserScore>, String, UserScore>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;
					public Tuple2<String, UserScore> call(
							Tuple2<String, UserScore> records)
									throws Exception {

						String primaryKey = records._2.getTenantId() 
								+ DELIMITER_PROPERTY + records._2.getRegionId()
								+ DELIMITER_PROPERTY + records._1 ;
						return new Tuple2<String, UserScore>(primaryKey, records._2);
					}
				});
		return requiredFormatUserScoreRDD;
	}

	/**
	 * 
	 * This method recalculate the userScore if the user have the same movies in there score which are recommended to him
	 * @param dayScoreRDDPerUserMovie
	 * @return
	 */
	  
	public static JavaPairRDD<String, UserScore> calculateFinalUserMovieScoreByConsideringSelfReco(JavaPairRDD<String, Tuple2<UserScore, Optional<UserScore>>> joinResult) {
		JavaPairRDD<String, UserScore> finalUserScoreRDD = joinResult
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<UserScore, Optional<UserScore>>>, String, UserScore>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;
					UserScore finalUserScore = null;
					public Tuple2<String, UserScore> call(
							Tuple2<String, Tuple2<UserScore, Optional<UserScore>>> records)
									throws Exception {
						Map<String, Double> movieScore = new HashMap<String, Double>();
						String primary_key = records._1;
						
						if(records._2._2.isPresent()){
							for(String key : records._2._1.getMovieScore().keySet()){
								if(records._2._2.get().getMovieScore().keySet().contains(key))
								{
									Double newScore = records._2._1.getMovieScore().get(key)/2;
									movieScore.put(key, newScore);
								}else {
								movieScore.put(key, records._2._1.getMovieScore().get(key));
								}
							}
							records._2._1.setMovieScore(movieScore);
						}

						finalUserScore = new UserScore(records._2._1.getTenantId(), records._2._1.getRegionId(),
								primary_key, records._2._1.getMovieScore(), records._2._1.getEventTypeAggregate());
						return new Tuple2<String, UserScore>(primary_key , finalUserScore);
					}

				});

		return finalUserScoreRDD;
	}	 

}
