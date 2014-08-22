package com.dla.foundation.services.queue.filter;

import java.io.Serializable;

import com.dla.foundation.data.persistence.SimpleFoundationEntity;

/**
 * Filters contain pre-processing or filtering logic
 * that must be executed before Updater writes event to its end-point 
 * 
 * The chain of filter(s) must be specified as 'updater_filters' value
 * in QueueListenerConfig file.
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public interface Filter extends Serializable {

	<TEntity extends SimpleFoundationEntity> TEntity doFilter(TEntity e) throws FilterException;

}
