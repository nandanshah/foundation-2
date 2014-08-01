package com.dla.foundation.eo.entities.userEvent;

import java.util.UUID;

import com.dla.foundation.eo.entities.Event;

public class UserItemDeleteToWatchList extends Event {
	public UUID profileID;
	public UUID itemID;
}
