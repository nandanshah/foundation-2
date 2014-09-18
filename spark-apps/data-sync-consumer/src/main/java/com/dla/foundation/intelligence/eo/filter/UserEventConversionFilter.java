package com.dla.foundation.intelligence.eo.filter;

import com.dla.foundation.data.entities.event.Event;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;
import com.dla.foundation.intelligence.entity.UserEvent;

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
		UserEvent ue = null;
		Event ee = (Event) e;
		ue = UserEvent.copy(ee);
		return (TEntity) ue;
	}
}
