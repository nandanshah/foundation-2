package com.dla.foundation.useritemreco.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.lang.time.DateUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class UserItemRecommendationUtil {

	private static final Integer REQUIRED_EVENT_VALUE = 1;
	private static final String EVENT_REQUIRED = "eventrequired";
	private static final String DATE = "date";
	private static final String LAST_RECO_FETCHED = "lastrecofetched";
	private static final String DELIMITER = "#";

	public static long getFormattedDate(long time) {
		Calendar date = new GregorianCalendar();
		date.setTimeInMillis(time);
		date.set(Calendar.HOUR_OF_DAY, 0);
		date.set(Calendar.MINUTE, 0);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MILLISECOND, 0);
		return date.getTimeInMillis();
	}

	public static String[] getList(String value, String delimiter) {

		return value.split(delimiter);

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

	public static Date getDate(String date, String dateFormat)
			throws ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		return simpleDateFormat.parse(date);
	}

	public static String getWhereClause(Date startDate) {
		long startTimestamp = getFormattedDate(startDate.getTime());
		long endTimestamp = getFormattedDate(DateUtils.addDays(startDate, 1)
				.getTime());

		return EVENT_REQUIRED + "=" + REQUIRED_EVENT_VALUE + " and " + DATE
				+ " >= " + startTimestamp + " and " + DATE + "< "
				+ endTimestamp;

	}

	public static String getWhereClause(Date startDate, String appType) {
		long startTimestamp = getFormattedDate(startDate.getTime());
		long endTimestamp = getFormattedDate(DateUtils.addDays(startDate, 1)
				.getTime());
		if (PropKeys.SOCIAL_RECOMMENDATION.getValue().equalsIgnoreCase(appType)
				|| appType == null) {
			return EVENT_REQUIRED + "=" + REQUIRED_EVENT_VALUE + " and " + DATE
					+ " >= " + startTimestamp + " and " + DATE + "< "
					+ endTimestamp;
		}
		if (PropKeys.PIO_RECOMMENDATION.getValue().equalsIgnoreCase(appType)) {
			return EVENT_REQUIRED + "=" + REQUIRED_EVENT_VALUE + " and "
					+ LAST_RECO_FETCHED + " >= " + startTimestamp + " and "
					+ LAST_RECO_FETCHED + "< " + endTimestamp;
		}
		return null;
	}

	public static JavaPairRDD<String, String> mergeTenant(
			JavaPairRDD<String, String> profileRDD,
			JavaPairRDD<String, String> accountRDD) {
		JavaPairRDD<String, Tuple2<String, String>> profileWithTmpRDD = profileRDD
				.join(accountRDD);
		JavaPairRDD<String, String> profileWithTenantRDD = profileWithTmpRDD
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {


					private static final long serialVersionUID = 8059711385731707290L;

					public Tuple2<String, String> call(
							Tuple2<String, Tuple2<String, String>> record)
									throws Exception {
						String[] keys = record._2._1.split(DELIMITER);
						String regionId = keys[0];
						String profileId = keys[1];
						String tenantId = record._2._2;
						String primaryKey = tenantId + DELIMITER + regionId;
						return new Tuple2<String, String>(primaryKey, profileId);
					}
				});
		return profileWithTenantRDD;
	}
}
