package com.dla.foundation.trendReco.model;

import java.io.Serializable;
import java.util.Map;

public class UserSummary implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1323109724398361790L;
	private int tenantId;
	private int regionId;
	private int itemId;
	private long timestamp;
	private int userId;
	private Map<Integer, Integer> eventTypeAggregate;
	private double dayScore;

	public UserSummary(int tenantId, int regionId, int itemId, long timestamp,
			int userId, Map<Integer, Integer> eventTypeAggregate,
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

	public int getTenantId() {
		return tenantId;
	}

	public int getRegionId() {
		return regionId;
	}

	public int getItemId() {
		return itemId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getUserId() {
		return userId;
	}

	public Map<Integer, Integer> getEventTypeAggregate() {
		return eventTypeAggregate;
	}

	public double getDayScore() {
		return dayScore;
	}

	public void setTenantId(int tenantId) {
		this.tenantId = tenantId;
	}

	public void setRegionId(int regionId) {
		this.regionId = regionId;
	}

	public void setItemId(int itemId) {
		this.itemId = itemId;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public void setEventTypeAggregate(Map<Integer, Integer> eventTypeAggregate) {
		this.eventTypeAggregate = eventTypeAggregate;
	}

	public void setDayScore(double dayScore) {
		this.dayScore = dayScore;
	}

}
