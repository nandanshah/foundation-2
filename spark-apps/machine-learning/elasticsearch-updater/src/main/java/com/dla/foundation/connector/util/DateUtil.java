package com.dla.foundation.connector.util;

import java.util.Calendar;
import java.util.Date;

public class DateUtil {

	public static long getPreviousDay(){
		Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        Date yesterday = cal.getTime();
		cal.setTimeInMillis(yesterday.getTime());
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		
		return cal.getTimeInMillis();
	}
}
