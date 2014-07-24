package com.dla.foundation.trendReco.model;

import java.io.Serializable;
import java.util.Map;

public class UserSummary implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1323109724398361790L;
	private String tenantId;
	private String regionId;
	private String itemId;
	private long timestamp;
	private String userId;
	private Map<String, Integer> eventTypeAggregate;
	private double dayScore;

	public UserSummary(String tenantId, String regionId, String itemId, long timestamp,
			String userId, Map<String, Integer> eventTypeAggregate,
			double dayScore) {
		super();
		this.tenantId = tenantId;
		this.regionId = regionId;
		this.itemId = itemId;
		this.timestamp = timestamp;
		this.userId = userId;
		this.eventTypeAggregate = eventTypeAggregate;
		this.dayScore = dayScore;
	}

	public UserSummary() {
		super();

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

	public long getTimestamp() {
		return timestamp;
	}

	public String getUserId() {
		return userId;
	}

	public Map<String, Integer> getEventTypeAggregate() {
		return eventTypeAggregate;
	}

	public double getDayScore() {
		return dayScore;
	}

	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}

	public void setRegionId(String regionId) {
		this.regionId = regionId;
	}

	public void setItemId(String itemId) {
		this.itemId = itemId;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public void setEventTypeAggregate(Map<String, Integer> eventTypeAggregate) {
		this.eventTypeAggregate = eventTypeAggregate;
	}

	public void setDayScore(double dayScore) {
		this.dayScore = dayScore;
	}

}
