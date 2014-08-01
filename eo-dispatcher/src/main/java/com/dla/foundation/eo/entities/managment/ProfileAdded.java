package com.dla.foundation.eo.entities.managment;

import java.util.UUID;

import com.dla.foundation.eo.entities.Event;

public class ProfileAdded extends Event {
	public UUID profileID;
	public UUID preferredRegionID;
	public UUID PreferredLocaleID;
}
