package com.dla.foundation.trendReco.util;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.time.DateUtils;

import com.dla.foundation.trendReco.model.EventType;

public class TrendRecommendationUtil implements Serializable {

	private static final Integer REQUIRED_EVENT_VALUE = 1;
	private static final String EVENTREQUIRED = "eventrequired";
	private static final String DATE = "date";

	/**
	 * 
	 */
	private static final long serialVersionUID = -652989078194260542L;

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

	public static Date getDate(String date, String dateFormat)
			throws ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		return simpleDateFormat.parse(date);
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

	public static List<Date> getAllDates(Date startDate, Date endDate,
			int recalPeriod) {
		List<Date> dates = new ArrayList<>();
		Date tmpDate = startDate;
		dates.add(tmpDate);
		tmpDate = DateUtils.addDays(tmpDate, recalPeriod);
		while (tmpDate.before(endDate) || tmpDate.equals(endDate)) {

			tmpDate = DateUtils.addDays(tmpDate, recalPeriod);
			dates.add(tmpDate);
		}
		return dates;
	}

	public static String getWhereClause(Date startDate) {
		long startTimestamp = TrendRecommendationUtil
				.getFormattedDate(startDate.getTime());
		long endTimestamp = TrendRecommendationUtil.getFormattedDate(DateUtils
				.addDays(startDate, 1).getTime());
		return EVENTREQUIRED + "=" + REQUIRED_EVENT_VALUE + " and " + DATE + " >= "
				+ startTimestamp + " and " + DATE + "< " + endTimestamp;
	}

	public static String getWhereClause(Date startDate, Date endDate) {
		long startTimestamp = TrendRecommendationUtil
				.getFormattedDate(startDate.getTime());
		long endTimestamp = TrendRecommendationUtil.getFormattedDate(DateUtils.addDays(endDate, 1)
				.getTime());
		return EVENTREQUIRED + "=" + REQUIRED_EVENT_VALUE + " and " + DATE + " >= "
				+ startTimestamp + " and " + DATE + "< " + endTimestamp;
	}

	public static String getWhereClause(Date startDate, int historyPeriod) {
		long startTimestamp = TrendRecommendationUtil
				.getFormattedDate(startDate.getTime());
		long endTimestamp = TrendRecommendationUtil.getFormattedDate(DateUtils
				.addDays(startDate, (-1) * historyPeriod).getTime());

		return EVENTREQUIRED + "=" + REQUIRED_EVENT_VALUE + " and " + DATE + " >= "
				+ endTimestamp + " and " + DATE + "<= " + startTimestamp;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, EventType> getRequiredEvent(String value)
			throws NumberFormatException {
		Map<String, EventType> requiredEvent = new HashedMap();
		EventType eventType;
		
		String[] events = value.split("\\|");
		requiredEvent = new HashedMap();
		
		for (String event : events) {
			String[] record = event.split(",");
			if (record.length == 3) {
				
				String[] values = record[2].split("#");
				eventType = new EventType(record[0].trim().toLowerCase(), Integer.parseInt(values[1]),Integer.parseInt(record[1]));
			} else {
				eventType = new EventType(record[0].trim().toLowerCase(),Integer.parseInt(record[1].trim()));
			}

			requiredEvent.put(record[0].toLowerCase(), eventType);

		}
		return requiredEvent;

	}

	public static Class<?> getService(String className, Class<?> IClass)
			throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException, ClassNotFoundException {

		Class<?> trendScoreService = Class.forName(className)
				.asSubclass(IClass);

		return trendScoreService;

	}

}
