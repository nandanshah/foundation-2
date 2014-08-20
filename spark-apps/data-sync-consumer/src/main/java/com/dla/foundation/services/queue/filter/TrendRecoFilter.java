package com.dla.foundation.services.queue.filter;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import com.dla.foundation.data.entities.analytics.UserEvent;

public class TrendRecoFilter implements Filter, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6030631656954886213L;

	@Override
	public void doFilter(UserEvent e) {
		e.eventrequired = 1;
		e.date = new Date(getFormattedDate(e.timestamp));
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
}
