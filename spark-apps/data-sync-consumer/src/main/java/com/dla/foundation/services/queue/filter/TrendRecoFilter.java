package com.dla.foundation.services.queue.filter;

import com.dla.foundation.data.entities.analytics.UserEvent;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;

/**
 * Sets a flag in UserEvent to indicate whether that event
 * is required for Trend recommendation calculation or not.
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class TrendRecoFilter implements Filter {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6586760301417650259L;

	@Override
	public <TEntity extends SimpleFoundationEntity> TEntity doFilter(TEntity e)
			throws FilterException {
		((UserEvent) e).eventrequired = 1;
		return (TEntity) e;
	}
}
