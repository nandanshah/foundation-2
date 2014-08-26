package com.dla.foundation.trendReco.model;

import java.io.Serializable;
import java.util.Map;

public class DayScore implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4635904137451689023L;
	private String itemId;
	private long timestamp;
	private String tenantId;
	private String regionId;
	private Map<String, Double> eventTypeAggregate;
	private double dayScore;

	public DayScore(String tenantId, String regionId, long timestamp, String movieId,
			Map<String, Double> eventTypeAggregate, double dayScore) {
	
		this.itemId = movieId;
		this.timestamp = timestamp;
		this.tenantId = tenantId;
		this.regionId = regionId;
		this.eventTypeAggregate = eventTypeAggregate;
		this.dayScore = dayScore;
	}

	public DayScore(String tenantId, String regionId, String movieId,
			Map<String, Double> eventTypeAggregate, double dayScore) {
		this(tenantId, regionId, -1, movieId, eventTypeAggregate, dayScore);
	}

	public DayScore() {
		super();
	}

	public double getDayScore() {
		return dayScore;
	}

	public void setItemId(String movieId) {
		this.itemId = movieId;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}

	public void setRegionId(String regionId) {
		this.regionId = regionId;
	}

	public void setDayScore(double dayScore) {
		this.dayScore = dayScore;
	}

	public DayScore(String tenantId, String regionId, String movieId, long timestamp,
			Map<String, Double> eventTypeAggregate) {
		this(tenantId, regionId, timestamp, movieId, eventTypeAggregate, 0);

	}

	
	public String getItemId() {
		return itemId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getTenantId() {
		return tenantId;
	}

	public String getRegionId() {
		return regionId;
	}

	public Map<String, Double> getEventTypeAggregate() {
		return eventTypeAggregate;
	}

	public void setEventTypeAggregate(Map<String, Double> eventTypeAggregate) {
		this.eventTypeAggregate = eventTypeAggregate;
	}

}
