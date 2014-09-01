package com.dla.foundation.connector.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateUtil {

	public static String dateFormat = "yyyy-mm-dd";
/*	public static long getPreviousDay(Date date){
		
		
		
		Calendar cal = ;
        cal.add(Calendar.DATE, -1);
       // Date yesterday = cal.getTime();
		cal.setTimeInMillis(yesterday.getTime());
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		
		return cal.getTimeInMillis();
	}*/
	
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
	
	public static Date getDate(String date) throws ParseException{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		return simpleDateFormat.parse(date);
	}
	
	public static long getDateInLong(String date, String dateFormat)
			throws ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		return simpleDateFormat.parse(date).getTime();
	}

	public static Date getDate(String date, String dateFormat)
			throws ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		return simpleDateFormat.parse(date);
	}
	public static Date getDateInDate(String date, String dateFormat)
			throws ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		return simpleDateFormat.parse(date);
	}

	public static String getDateInFormat(Date date,String format){
		SimpleDateFormat dateFormat = new SimpleDateFormat(format);
		return dateFormat.format(date);
				
	}
}
