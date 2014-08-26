package com.dla.foundation.trendReco.model;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public class UserEvent implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -808961715382376700L;

	private String userId;
	private String itemId;
	private String eventType;
	private Date date;
	private String tenantId;
	private String regionId;
	private double playPercentage=-1;
	private double rateScore=-1;
	
	public double getPlayPercentage() {
		return playPercentage;
	}

	public void setPlayPercentage(double playPercentage) {
		this.playPercentage = playPercentage;
	}
	
	public double getRatescore() {
		return rateScore;
	}

	public void setRatescore(double rateScore) {
		this.rateScore = rateScore;
	}

	public String getItemid() {
		return itemId;
	}

	public void setItemid(String itemid) {
		this.itemId = itemid;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String event_type) {
		this.eventType = event_type;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String getTenantId() {
		return tenantId;
	}

	public void setTenantId(String tenantID) {
		this.tenantId = tenantID;
	}

	public String getRegionId() {
		return regionId;
	}

	public void setRegionId(String regionId) {
		this.regionId = regionId;
	}

}
