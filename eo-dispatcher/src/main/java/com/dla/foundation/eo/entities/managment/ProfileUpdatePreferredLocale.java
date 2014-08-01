package com.dla.foundation.eo.entities.managment;

import java.util.UUID;

import com.dla.foundation.eo.entities.Event;

public class ProfileUpdatePreferredLocale extends Event {
	public UUID profileID;
	public UUID PreferredLocaleID;
}
