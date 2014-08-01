package com.dla.foundation.eo.entities.userEvent;

import java.util.UUID;

import com.dla.foundation.eo.entities.Event;
import com.dla.foundation.eo.entities.SearchType;

public class UserSearch extends Event {
	public UUID profileID;
	public SearchType searchType;
	public String searchQuery;
}
