package com.dla.foundation.services.queue.filter;

import com.dla.foundation.data.entities.analytics.UserEvent;
import com.dla.foundation.data.entities.event.Event;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;

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
