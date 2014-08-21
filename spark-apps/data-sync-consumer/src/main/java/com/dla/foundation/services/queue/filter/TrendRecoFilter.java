package com.dla.foundation.services.queue.filter;

import com.dla.foundation.data.entities.analytics.UserEvent;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;

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
