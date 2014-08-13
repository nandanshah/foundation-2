package com.dla.foundation.services.queue.updater;

import java.util.ArrayList;

import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;
import com.dla.foundation.services.queue.filter.Filter;
import com.dla.foundation.services.queue.util.UpdaterConfig;


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
	 */
	final public AnalyticsCollectionEvent updateSyncEvent(AnalyticsCollectionEvent event) {
		filterEvent(event, conf.filters);
		return doUpdateSyncEvent(event);
	}

	/**
	 * Write event to endpoint, does not return anything
	 * 
	 * @param event Event to be written to endpoint
	 */
	final public void updateAsyncEvent(AnalyticsCollectionEvent event) {
		filterEvent(event, conf.filters);
		doUpdateAsyncEvent(event);
	}

	/**
	 * Release resources used by updater if any
	 */
	abstract void close();

	protected abstract void filterEvent(AnalyticsCollectionEvent event, ArrayList<Filter> filters);

	protected abstract AnalyticsCollectionEvent doUpdateSyncEvent(AnalyticsCollectionEvent event);

	protected abstract void doUpdateAsyncEvent(AnalyticsCollectionEvent event);
}
