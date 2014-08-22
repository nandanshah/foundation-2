package com.dla.foundation.services.queue.filter;

import com.dla.foundation.data.entities.analytics.UserEvent;
import com.dla.foundation.data.entities.event.Event;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;

/**
 * Use this filter to transform global Event into 
 * Foundation Intelligence System specific UserEvent.
 * 
 * If another filter expects UserEvent, this filter must be called
 * before that filter can be used.
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class UserEventConversionFilter implements Filter {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7291845249055249462L;

	@SuppressWarnings("unchecked")
	@Override
	public <TEntity extends SimpleFoundationEntity> TEntity doFilter(TEntity e)
			throws FilterException {
		Event ee = (Event) e;
		UserEvent ue = UserEvent.copy(ee);
		return (TEntity) ue;
	}
}
