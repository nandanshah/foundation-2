package com.dla.foundation.trendReco.model;

import java.io.Serializable;
import java.util.Map;

public class DayScore implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4635904137451689023L;
	private int itemId;
	private long timestamp;
	private int tenantId;
	private int regionId;
	private Map<Integer, Integer> eventTypeAggregate;
	private double dayScore;

	public DayScore(int tenantId, int regionId, long timestamp, int movieId,
			Map<Integer, Integer> eventTypeAggregate, double dayScore) {
		super();
		this.itemId = movieId;
		this.timestamp = timestamp;
		this.tenantId = tenantId;
		this.regionId = regionId;
		this.eventTypeAggregate = eventTypeAggregate;
		this.dayScore = dayScore;
	}

	public DayScore(int tenantId, int regionId, int movieId,
			Map<Integer, Integer> eventTypeAggregate, double dayScore) {
		this(tenantId, regionId, -1, movieId, eventTypeAggregate, dayScore);
	}

	public DayScore() {
		super();
	}

	public double getDayScore() {
		return dayScore;
	}

	public void setItemId(int movieId) {
		this.itemId = movieId;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setTenantId(int tenantId) {
		this.tenantId = tenantId;
	}

	public void setRegionId(int regionId) {
		this.regionId = regionId;
	}

	public void setDayScore(double dayScore) {
		this.dayScore = dayScore;
	}

	public DayScore(int tenantId, int regionId, int movieId, long timestamp,
			Map<Integer, Integer> eventTypeAggregate) {
		this(tenantId, regionId, timestamp, movieId, eventTypeAggregate, 0);

	}

	public int getItemId() {
		return itemId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getTenantId() {
		return tenantId;
	}

	public int getRegionId() {
		return regionId;
	}

	public Map<Integer, Integer> getEventTypeAggregate() {
		return eventTypeAggregate;
	}

	public void setEventTypeAggregate(Map<Integer, Integer> eventTypeAggregate) {
		this.eventTypeAggregate = eventTypeAggregate;
	}

}
