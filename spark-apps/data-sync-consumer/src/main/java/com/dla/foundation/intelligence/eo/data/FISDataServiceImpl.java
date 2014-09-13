package com.dla.foundation.intelligence.eo.data;

import java.util.UUID;

import com.dla.foundation.data.FoundationDataContext;
import com.dla.foundation.data.PageResult;
import com.dla.foundation.intelligence.entity.UserEvent;

public class FISDataServiceImpl implements FISDataService {

	final private FoundationDataContext data;

	public FISDataServiceImpl(FoundationDataContext data) {
		this.data = data;
	}

	private boolean suppressReselect = false;

	@Override
	public void suppressReselectOnInserts(boolean suppress) {
		suppressReselect = suppress;
	}

	@Override
	public UserEvent getUserEvent(UUID id) {
		if (id == null) {
			throw new IllegalArgumentException("userEventId must not be null");
		}

		UserEvent selectUserEvent = new UserEvent();
		selectUserEvent.id = id;
		UserEvent selectedItem = data.select(selectUserEvent);

		return selectedItem;
	}

	@Override
	public UserEvent insertOrUpdateUserEvent(UserEvent event) {
		return data.insertOrUpdate(event, suppressReselect);
	}

	@Override
	public PageResult<UUID, UserEvent> getAllUserEvents(UUID offsetId, int size) {
		return data.selectPage(UserEvent.class, offsetId, size);
	}
}
