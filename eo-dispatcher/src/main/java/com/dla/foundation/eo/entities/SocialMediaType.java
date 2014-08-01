package com.dla.foundation.eo.entities;
public enum SocialMediaType {
	FACEBOOK("facebook"), TWEETER("tweeter"), GOOGLE_PLUS("google_plus"), MOBILI(
			"mobili");
	private String mediaType;

	private SocialMediaType(String mediaType) {
		this.mediaType = mediaType;
	}

	public String getMediaType() {
		return mediaType;
	}

}
