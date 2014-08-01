package com.dla.foundation.eo.entities.userEvent;

import java.util.UUID;

import com.dla.foundation.eo.entities.DeviceType;
import com.dla.foundation.eo.entities.Event;
import com.dla.foundation.eo.entities.ImpressionSource;
import com.dla.foundation.eo.entities.NetworkType;

public class UserItemPlayStart extends Event {
	public UUID profileID;
	public UUID itemID;
	public ImpressionSource impressionSource;
	public DeviceType deviceType; 
	public UUID regionID;
	public UUID localeID;
	public NetworkType networkType;

}
