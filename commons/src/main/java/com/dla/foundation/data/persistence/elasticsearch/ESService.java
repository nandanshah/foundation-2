package com.dla.foundation.data.persistence.elasticsearch;

import java.io.IOException;

import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;

public interface ESService {
			
	void addItem(AnalyticsCollectionEvent event) throws IOException;
	void deleteItem(AnalyticsCollectionEvent event) throws IOException;
	void updateItem(AnalyticsCollectionEvent event) throws IOException;
	void postBulkEvents(String bulkevents);
}
