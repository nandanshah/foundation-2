package com.dla.foundation.eo.entities.managment;

import java.util.UUID;

import com.dla.foundation.eo.entities.Event;
import com.dla.foundation.eo.entities.SocialMediaType;

public class ProfileUpdateSocialMediaAccountDelete extends Event {
	public UUID profileID;
	public SocialMediaType socialMediaType;
	public String gigyaAuthToken;

}
