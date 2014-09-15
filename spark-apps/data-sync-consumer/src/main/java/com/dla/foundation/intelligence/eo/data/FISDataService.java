package com.dla.foundation.intelligence.eo.data;

import java.util.UUID;

import com.dla.foundation.data.PageResult;
import com.dla.foundation.intelligence.entity.UserEvent;

public interface FISDataService {
	
	void suppressReselectOnInserts(boolean suppress);
	UserEvent getUserEvent(UUID id);
	UserEvent insertOrUpdateUserEvent(UserEvent event);
	PageResult<UUID, UserEvent> getAllUserEvents(UUID offsetId, int size);

}
