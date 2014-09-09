package com.dla.foundation.intelligence.eo.filter;

import com.dla.foundation.data.entities.event.Event;
import com.dla.foundation.data.entities.event.EventType;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;

/**
 * Filters out unsupported events by ElasticSearch Updater
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class ESEventFilter implements Filter {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8510336502460659862L;

	@Override
	public <TEntity extends SimpleFoundationEntity> TEntity doFilter(TEntity e)
			throws FilterException {
		if(((Event) e).eventType!=EventType.ProfileDeleted) {
			throw new UnsupportedEventException("Input event type is not supported by updater: " + ((Event) e).eventType);
		}
		return e;
	}
}
