package com.dla.foundation.socialReco;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.dla.foundation.socialReco.model.SocialScore;
import com.dla.foundation.socialReco.model.UserScore;
import com.dla.foundation.socialReco.util.SocialRecoPostprocessing;

public class ProfileCalc implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final String DELIMITER_PROPERTY = "#";
	
	public static JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> regionCalculations(JavaPairRDD<String, UserScore> dayScoreRDDPerUserMovie, 
			JavaPairRDD<String, String> profileInfo,final Integer topEntryCnt,final long timestamp){
		
		//Convert the PrimaryKey of dayScoreRDDPerUserMovie to ProfileId from TenantId#RegionId#ProfileId
		JavaPairRDD<String, UserScore> requiredPrimaryKey = changePrimaryKey(dayScoreRDDPerUserMovie);
		
		//Join dayScoreRDDPerUserMovie with profileInfo
		JavaPairRDD<String, Tuple2<UserScore,String>> joinedData=requiredPrimaryKey.join(profileInfo);
		
		// Filter if RegionId from dayScoreRDDPerUserMovie with profileInfo are equal then return Entry
		JavaPairRDD<String, Tuple2<UserScore,String>> filteredRDD= filterBasedOnRegion(joinedData);
		
		// Convert the data to JPRDD<String, UserScore>
		JavaPairRDD<String, UserScore> requiredRDD= convertToReaquiredRDD(filteredRDD);
		
		//Get top n records in sorted order based on movies score. ...
		JavaPairRDD<String, UserScore> topRecords= getTopMovies(requiredRDD, topEntryCnt );
		
		// Normalization of Score ...
		JavaPairRDD<String, UserScore> normalizedScore= getNormalizedMovieScore(topRecords);
		
		//JavaPairRDD<String, Social> finalScore= finalScore(normalizedScore);
		JavaRDD<SocialScore> normalizedItemTrendScoreRDD= finalScore(normalizedScore, timestamp);
		
		//Post-processing of data to store it in Cassandra.
		JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> finalScore = SocialRecoPostprocessing.processingForSocialScore(normalizedItemTrendScoreRDD);
			
		return finalScore;
			
	}
	
	/**
	 * 
	 * This methods return the top n records in sorted order based on movies score.
	 * 
	 * @param requiredRDD
	 * @param topEntryCnt
	 * @return
	 */
	private static JavaPairRDD<String, UserScore> getTopMovies(
			JavaPairRDD<String, UserScore> requiredRDD, final Integer topEntryCnt ) {
		JavaPairRDD<String, UserScore> topRecords = requiredRDD
				.mapToPair(new PairFunction<Tuple2<String,UserScore>, String, UserScore>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, UserScore> call(
							Tuple2<String, UserScore> records) throws Exception {
						
						String primaryKey =  records._1;
						UserScore userScore = null;
						Map<String, Double> movieScore = new HashMap<String, Double>();
						Map<String, Map<String, Double>>  eventTypeAggregate = new HashMap<String, Map<String, Double>>();
					
						movieScore = getTopMovieScore(records._2.getMovieScore(), topEntryCnt );
												
						for(String key : movieScore.keySet())
						{
							eventTypeAggregate.put( key, records._2.getEventTypeAggregate().get(key));
						}
						userScore = new UserScore(records._2.getTenantId(),records._2.getRegionId(), primaryKey,
								movieScore, eventTypeAggregate);
						
						return new Tuple2<String, UserScore>(primaryKey, userScore);
					}
				
				});
		
		return topRecords;
	}

	/**
	 * 
	 * This methods return the top n records in sorted order based on movies score.
	 * 
	 * @param movieScore
	 * @param topEntryCnt
	 * @return
	 * @throws IOException
	 */
	private static Map<String, Double> getTopMovieScore(
			Map<String, Double> movieScore,final Integer topEntryCnt ) throws IOException {
		
	      LinkedList<Entry<String, Double>> largestList = new LinkedList<Entry<String, Double>>(movieScore.entrySet());
	      Collections.sort(largestList, new Comparator<Entry<String,Double>>(){
	    	  public int compare(Entry<String, Double>o1, Entry<String, Double>o2)
	    	  {
	    		  return o1.getValue().compareTo(o2.getValue());
	    	  }
	      });
	    
	      Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
	      Integer count = 0;
	      for(Iterator<Entry<String,Double>> itr = largestList.iterator(); itr.hasNext();)
	      {
	    	  if(count>=topEntryCnt)
	    	  {
		    	 break;
	    	  }Map.Entry<String, Double> entry = (Map.Entry<String, Double>)itr.next();
		    	  sortedMap.put(entry.getKey(), entry.getValue());
		    	  count++;	    	 
	      }
	   return sortedMap;
	}
	
	/**
	 * 
	 * This method convert the data into required format.
	 * 
	 * @param filteredRDD
	 * @return
	 */
	public static JavaPairRDD<String, UserScore> convertToReaquiredRDD(JavaPairRDD<String, Tuple2<UserScore,String>> filteredRDD){
		JavaPairRDD<String, UserScore> requiredRDD = filteredRDD
				.mapToPair(new PairFunction<Tuple2<String,Tuple2<UserScore,String>>, String, UserScore>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, UserScore> call(
							Tuple2<String, Tuple2<UserScore, String>> records)
							throws Exception {
						String primaryKey =  records._1;
						return new Tuple2<String, UserScore>(primaryKey, records._2._1);
					}
					
		});
		return requiredRDD;
	}
	
	/**
	 * 
	 * This method Convert the PrimaryKey of dayScoreRDDPerUserMovie to ProfileId from TenantId#RegionId#ProfileId
	 * 
	 * @param userScore
	 * @return
	 */
	
	public static JavaPairRDD<String, UserScore> changePrimaryKey(JavaPairRDD<String, UserScore> userScore){
		
		JavaPairRDD<String, UserScore> requiredFormatUserScoreRDD = userScore
		.mapToPair(new PairFunction<Tuple2<String, UserScore>, String, UserScore>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			public Tuple2<String, UserScore> call(
					Tuple2<String, UserScore> records)
							throws Exception {
				
				String[] keys = records._1.split(DELIMITER_PROPERTY);
				String primaryKey = keys[2];
				
				return new Tuple2<String, UserScore>(primaryKey, records._2);
				
			}
		});
		return requiredFormatUserScoreRDD;
	}
	
	/**
	 * 
	 * This method filetr the records based on regionID. If regionId of movie is same as regionId of profile
	 * then only recommended this movie to that profile.
	 * 
	 * @param scoreRDD
	 * @return
	 */
	public static JavaPairRDD<String, Tuple2<UserScore,String>> filterBasedOnRegion(
			JavaPairRDD<String, Tuple2<UserScore,String>> scoreRDD) {
		JavaPairRDD<String, Tuple2<UserScore,String>> filteredScoreRDD = scoreRDD
				.filter(new Function<Tuple2<String, Tuple2<UserScore,String>>, Boolean>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -6160930803403593402L;

					@Override
					public Boolean call(
							Tuple2<String, Tuple2<UserScore, String>> record)
							throws Exception {
						if(record._2._1.getRegionId().equals(record._2._2))
						{
							return true;
						}
						// TODO Auto-generated method stub
						return false;
					}
				});

		return filteredScoreRDD;
	}
	
	/**
	 * 
	 * This method return the score in proper format (2 numbers after decimal digit )
	 * 
	 * @param topRecords
	 * @return
	 */
	private static JavaPairRDD<String, UserScore> getNormalizedMovieScore(
			JavaPairRDD<String, UserScore> topRecords) {
		JavaPairRDD<String, UserScore> normalizedScore = topRecords.
				mapToPair(new PairFunction<Tuple2<String,UserScore>, String, UserScore>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, UserScore> call(
							Tuple2<String, UserScore> records) throws Exception {
						Map<String, Double> movieScore = new HashMap<String, Double>();
						Collection c= records._2.getMovieScore().values();
						Double maxValue = Collections.max(c);
						for(String key : records._2.getMovieScore().keySet())
						{
							movieScore.put(key, (double) Math.round((records._2.getMovieScore().get(key)/maxValue)*100)/100);
						}
						UserScore userScore = new UserScore(records._2.getTenantId(),records._2.getRegionId(), records
								._1, movieScore, records._2.getEventTypeAggregate());
						
						return new Tuple2<String, UserScore>(records._1, userScore);
					}
				});
		return normalizedScore;
	}
	
	/**
	 * 
	 * This method convert the data in SocialScore format.
	 * 
	 * @param normalizedScore
	 * @param timestamp
	 * @return
	 */
	private static JavaRDD<SocialScore> finalScore(
			JavaPairRDD<String, UserScore> normalizedScore, final long timestamp) {
		
		return normalizedScore.
				flatMap(new FlatMapFunction<Tuple2<String,UserScore>, SocialScore>(){

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;
					List< SocialScore> recommendItem = new ArrayList<SocialScore>();
					
					@Override
					public Iterable<SocialScore> call(
							Tuple2<String, UserScore> records) throws Exception {
						SocialScore socialScore = null;
						Map<String, Double> movieScore = new HashMap<String, Double>();
						movieScore = records._2.getMovieScore();
						for(String itemId : movieScore.keySet())
						{
							socialScore = new SocialScore(records._1, records._2.getTenantId(), records._2.getRegionId(),
									itemId, records._2.getMovieScore().get(itemId), timestamp , 
									records._2.getEventTypeAggregate().get(itemId).toString());
							
							recommendItem.add(socialScore);
						}
											
						return recommendItem;
					}
					
				});
				
	}

}
