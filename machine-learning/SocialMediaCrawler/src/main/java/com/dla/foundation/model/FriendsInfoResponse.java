package com.dla.foundation.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FriendsInfoResponse {
	public final Friend friends[];
	public final int statusCode;
	public final int errorCode;
	public final String statusReason;
	public final String callId;

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Friend {
		public final String UID;
		public final boolean isSiteUser;
		public final boolean isSiteUID;
		public final String nickname;
		public final String photoURL;
		public final String thumbnailURL;
		public final Identity identities[];

		public Friend() {
			this(null, false, false, null, null, null, null);
		}

		public Friend(String uID, boolean isSiteUser, boolean isSiteUID,
				String nickname, String photoURL, String thumbnailURL,
				Identity[] identities) {
			super();
			UID = uID;
			this.isSiteUser = isSiteUser;
			this.isSiteUID = isSiteUID;
			this.nickname = nickname;
			this.photoURL = photoURL;
			this.thumbnailURL = thumbnailURL;
			this.identities = identities;
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Identity {
		public final String provider;
		public final String providerUID;
		public final String isLoginIdentity;
		public final String nickname;
		public final String photoURL;
		public final String thumbnailURL;

		public Identity() {
			this(null, null, null, null, null, null);
		}

		public Identity(String provider, String providerUID,
				String isLoginIdentity, String nickname, String photoURL,
				String thumbnailURL) {
			super();
			this.provider = provider;
			this.providerUID = providerUID;
			this.isLoginIdentity = isLoginIdentity;
			this.nickname = nickname;
			this.photoURL = photoURL;
			this.thumbnailURL = thumbnailURL;
		}

	}

	public FriendsInfoResponse() {
		this(null, -1, -1, null, null);
	}

	public FriendsInfoResponse(Friend[] friends, int statusCode, int errorCode,
			String statusReason, String callId) {
		this.friends = friends;
		this.statusCode = statusCode;
		this.errorCode = errorCode;
		this.statusReason = statusReason;
		this.callId = callId;
	}

}
