package com.dla.foundation.services.queue.filter;

import java.io.Serializable;

import com.dla.foundation.data.persistence.SimpleFoundationEntity;

public interface Filter extends Serializable {

	<TEntity extends SimpleFoundationEntity> TEntity doFilter(TEntity e) throws FilterException;

}
