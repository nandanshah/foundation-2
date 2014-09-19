package com.dla.foundation.socialReco.util;

import java.util.List;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.socialReco.UserScoreCalc;
import com.dla.foundation.socialReco.model.CassandraConfig;
import com.dla.foundation.socialReco.model.DayScore;
import com.dla.foundation.socialReco.model.UserScore;

public class SocialRecommendationUtil  implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(SocialRecommendationUtil.class);
	private static final Integer REQUIRED_EVENT_VALUE = 1;
	private static final String FLAG = "eventrequired";
	private static final String DATE_FORMAT = "yyyy-MM-dd";
	private static final String TRUE = "true";
	private static final String FALSE = "false";

	private static final String DATE = "date";

	public static String[] getList(String value, String delimiter) {

		return value.split(delimiter);

	}
	
	public static String getWhereClause(Date startDate, Date endDate) {
		long endTimestamp = SocialRecommendationUtil
				.getFormattedDate(endDate.getTime());
		long startTimestamp = SocialRecommendationUtil
				.getFormattedDate(startDate.getTime());
		
		String whereClause = FLAG + "=" + REQUIRED_EVENT_VALUE + " and " + DATE + " <= "
				+ endTimestamp + " and " + DATE + ">= " + startTimestamp;
		return whereClause;
	}

	public static Date getDate(String date, String dateFormat)
			throws ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		return simpleDateFormat.parse(date);
	}

	public static long getFormattedDate(long time) {
		Calendar date = new GregorianCalendar();
		date.setTimeInMillis(time);
		date.set(Calendar.HOUR_OF_DAY, 0);
		date.set(Calendar.MINUTE, 0);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MILLISECOND, 0);
		return date.getTimeInMillis();
	}

	public static Date processInputDate(Date inputDate) {
		Calendar date = new GregorianCalendar();
		date.setTime(inputDate);
		date.set(Calendar.HOUR_OF_DAY, 0);
		date.set(Calendar.MINUTE, 0);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MILLISECOND, 0);

		return new Date(date.getTimeInMillis());
	}

	public static String getDate(Date date, String dateFormat)
			throws ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		return simpleDateFormat.format(date);
	}
	
	/**
	 * 
	 * This method reads the required data from common_daily_eventsummary_per_useritem table.
	 * Transform the data from ByteBuffer to Readable format and calculate the user score.
	 * @throws Exception 
	 * 
	 *  
	 **/

	public static JavaPairRDD<String, UserScore> readUserEventSummaryData(JavaSparkContext sparkContext,
			CassandraSparkConnector cassandraSparkConnector,CassandraConfig socialScoreCassandraProp, 
			final PropertiesHandler socialScoreProp) throws Exception {
		
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD;
		Configuration conf= new Configuration();

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date formattedYday, formattedWeek, startDate, endDate; 
		String socialRecoDateFlag = socialScoreProp
				.getValue(PropKeys.SOCIAL_RECO_DATE_FLAG.getValue());
		
		if (socialRecoDateFlag.toLowerCase().compareTo(TRUE) == 0) {
			logger.info("Executing socialRecoDate module");
			
			endDate = SocialRecommendationUtil.getDate(socialScoreProp
					.getValue(PropKeys.SOCIAL_RECO_DATE
							.getValue()), DATE_FORMAT);
			
			String weekToMonths = dateFormat.format(DateUtils.addDays(endDate, 
					-Integer.parseInt(socialScoreProp.getValue(PropKeys.NUMBER_OF_DAYS_FOR_MONTHS.getValue())) ));
			startDate = SocialRecommendationUtil.getDate(weekToMonths, DATE_FORMAT);

						
		} else if (socialRecoDateFlag.toLowerCase().compareTo(FALSE) == 0) {
			logger.info("Executing socialRecoDuration module");
					
			startDate = SocialRecommendationUtil.getDate(socialScoreProp
					.getValue(PropKeys.RECAL_START_DATE.getValue()), DATE_FORMAT);
			endDate = SocialRecommendationUtil.getDate(socialScoreProp
					.getValue(PropKeys.RECAL_END_DATE.getValue()), DATE_FORMAT);
		
			if(startDate.compareTo(endDate) >= 0)
			{ 
				throw new Exception("End date should be greater than Start Date");
			}
			
		} else {
			throw new Exception(
					"Please provide input date (input_date) for incremental processing or start date (start_date)"
					+ " for full recalculation with incremental_flag (true will be for incremental processing of "
					+ "input date and false will be for full recalculation from specified start date to end date");

		}
		
		logger.info("Retrieving Data from "+startDate+ "To " +endDate+ "Date" );
		String yesterday = dateFormat.format(DateUtils.addDays(endDate,
				-Integer.parseInt(socialScoreProp.getValue(PropKeys.NUMBER_OF_DAYS.getValue())) ));
		
		formattedYday = SocialRecommendationUtil.getDate(yesterday, DATE_FORMAT);

		// As we are using after method for Day score calculations... Added one more day in week 
		String dayToWeek = dateFormat.format(DateUtils.addDays(endDate, 
				-(Integer.parseInt(socialScoreProp.getValue(PropKeys.NUMBER_OF_DAYS_FOR_WEEKS.getValue()))+1) ));
		formattedWeek = SocialRecommendationUtil.getDate(dayToWeek, DATE_FORMAT);

		// It has where condition which allows it to fetch data only for the
		// provided date.
		cassandraRDD = cassandraSparkConnector.read(conf, sparkContext,
				socialScoreCassandraProp.getInputKeyspace(),
				socialScoreCassandraProp.getInputColumnfamily(),
				socialScoreCassandraProp.getPageRowSize(),
				SocialRecommendationUtil.getWhereClause(startDate,
						formattedYday)); 
		
	/*	if(cassandraRDD.count() == 0){
			throw new Exception("No user records present for given timeframe");
		}*/
	
		// Convert data into Readable format from ByteBuffer format
		JavaPairRDD<String, DayScore> readableFormatRDD = UserScoreTransformation
						.dataTransformation(cassandraRDD);
		
		logger.info("User Score Calculations");
		JavaPairRDD<String, UserScore> userScore= UserScoreCalc.userScoreCalculation(readableFormatRDD,
				formattedYday, formattedWeek, socialScoreProp);
	 	
		return userScore;
	}
	

}
