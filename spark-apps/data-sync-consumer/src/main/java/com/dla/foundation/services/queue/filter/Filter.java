package com.dla.foundation.services.queue.filter;

import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;

public interface Filter {
	
	void doFilter(AnalyticsCollectionEvent e);
	
}
