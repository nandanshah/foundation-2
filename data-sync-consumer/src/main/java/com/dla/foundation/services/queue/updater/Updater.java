package com.dla.foundation.services.queue.updater;

import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;


/**
 * Defines an interface that needs to be implemented by other endpoint specific updaters
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public interface Updater {
	/**
	 * Write event to endpoint and do return the response event
	 * 
	 * @param event Event to be written to endpoint
	 * @return Response event returned by updater
	 */
	AnalyticsCollectionEvent updateSyncEvent(AnalyticsCollectionEvent event);
	
	/**
	 * Write event to endpoint, does not return anything
	 * 
	 * @param event Event to be written to endpoint
	 */
	void updateAsyncEvent(AnalyticsCollectionEvent event);
	
	/**
	 * Release resources used by updater if any
	 */
	void close();
}
