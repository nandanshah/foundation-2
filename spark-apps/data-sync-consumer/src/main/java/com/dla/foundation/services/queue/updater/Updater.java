package com.dla.foundation.services.queue.updater;

import java.util.ArrayList;

import com.dla.foundation.data.entities.analytics.UserEvent;
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
	final public UserEvent updateSyncEvent(UserEvent event) {
		filterEvent(event, conf.filters);
		return doUpdateSyncEvent(event);
	}

	/**
	 * Write event to endpoint, does not return anything
	 * 
	 * @param event Event to be written to endpoint
	 */
	final public void updateAsyncEvent(UserEvent event) {
		filterEvent(event, conf.filters);
		doUpdateAsyncEvent(event);
	}

	/**
	 * Release resources used by updater if any
	 */
	abstract void close();

	protected abstract void filterEvent(UserEvent event, ArrayList<Filter> filters);

	protected abstract UserEvent doUpdateSyncEvent(UserEvent event);

	protected abstract void doUpdateAsyncEvent(UserEvent event);
}
