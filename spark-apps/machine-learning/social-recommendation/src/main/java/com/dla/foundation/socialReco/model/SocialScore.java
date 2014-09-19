package com.dla.foundation.socialReco.model;

import java.io.Serializable;
import java.util.Date;

public class SocialScore  implements Serializable {

	public  static String DELIM = "####";
	/**
	 * 
	 */
	private static final long serialVersionUID = -4236461259788692600L;
	long timestamp;
	String profileId;
	String tenantId;
	String regionId;
	String itemId;
	double socialScore;
	String reason;
	

	public SocialScore(String profileId, String tenantId, String regionId, String itemId, double socialscore,
			long timestamp, String reason) {
		super();
		this.profileId = profileId;
		this.tenantId = tenantId;
		this.regionId = regionId;
		this.itemId = itemId;
		this.socialScore = socialscore;
		this.timestamp = timestamp;
		this.reason = reason;
	}

	public long getTimestamp() {
		return timestamp;
	}
	
	public String getProfileId() {
		return profileId;
	}

	public String getTenantId() {
		return tenantId;
	}

	public String getRegionId() {
		return regionId;
	}

	public String getItemId() {
		return itemId;
	}

	public double getsocialScore() {
		return socialScore;
	}

	public void setSocialScore(double socialScore) {
		this.socialScore = socialScore;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}

	public String getReason() {
		return reason;
	}
	
	@Override
	public String toString() {
		return getTimestamp()+DELIM+getProfileId()+DELIM+getTenantId()+DELIM+getRegionId()+DELIM+getItemId()
				+DELIM+getsocialScore()+DELIM+getReason();
	}

}
