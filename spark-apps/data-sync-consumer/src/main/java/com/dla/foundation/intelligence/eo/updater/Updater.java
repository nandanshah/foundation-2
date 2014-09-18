package com.dla.foundation.intelligence.eo.updater;

import java.util.ArrayList;

import com.dla.foundation.data.persistence.SimpleFoundationEntity;
import com.dla.foundation.intelligence.eo.filter.Filter;
import com.dla.foundation.intelligence.eo.filter.FilterException;
import com.dla.foundation.intelligence.eo.util.UpdaterConfig;


/**
 * Defines an interface that needs to be implemented by other endpoint specific updaters
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public abstract class Updater {

	public UpdaterConfig conf;

	/**
	 * Write event to endpoint and do return the response event
	 * 
	 * @param event Event to be written to endpoint
	 * @throws FilterException 
	 */
	final public <TEntity extends SimpleFoundationEntity> TEntity updateSyncEvent(TEntity event) throws FilterException {
		return doUpdateSyncEvent(filterEvent(event, conf.filters));
	}

	/**
	 * Write event to endpoint, does not return anything
	 * 
	 * @param event Event to be written to endpoint
	 * @throws FilterException 
	 */
	final public <TEntity extends SimpleFoundationEntity> void updateAsyncEvent(TEntity event) throws FilterException {
		doUpdateAsyncEvent(filterEvent(event, conf.filters));
	}

	/**
	 * Release resources used by updater if any
	 */
	public abstract void close();

	protected abstract <TEntity extends SimpleFoundationEntity> TEntity filterEvent(TEntity event, ArrayList<Filter> filters) 
			throws FilterException;

	protected abstract <TEntity extends SimpleFoundationEntity> TEntity doUpdateSyncEvent(TEntity event);

	protected abstract <TEntity extends SimpleFoundationEntity> void doUpdateAsyncEvent(TEntity event);
}
