package com.dla.foundation.services.queue.filter;

import com.dla.foundation.data.entities.analytics.UserEvent;

public interface Filter {

	void doFilter(UserEvent e);

}
