package com.dla.foundation.services.queue.filter;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import com.dla.foundation.data.entities.analytics.UserEvent;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;

/**
 * Sets day of UserEvent. All events generated on particular
 * date will be assigned midnights time and date.
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class DateSetterFilter implements Filter, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6933684322407681736L;

	@Override
	public <TEntity extends SimpleFoundationEntity> TEntity doFilter(TEntity e)
			throws FilterException {
		((UserEvent) e).date = new Date(getFormattedDate(((UserEvent) e).timestamp));
		return (TEntity) e;
	}

	private long getFormattedDate(long time) {
		Calendar date = new GregorianCalendar();
		date.setTimeInMillis(time);
		date.set(Calendar.HOUR_OF_DAY, 0);
		date.set(Calendar.MINUTE, 0);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MILLISECOND, 0);
		return date.getTimeInMillis();
	}
}
