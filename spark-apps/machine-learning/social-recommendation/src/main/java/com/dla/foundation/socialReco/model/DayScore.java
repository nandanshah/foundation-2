package com.dla.foundation.socialReco.model;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public class DayScore implements Serializable {

	public  static String DELIM = "####";
	/**
	 * 
	 */
	private static final long serialVersionUID = -4635904137451689023L;
	private String itemId;
	private long timestamp;
	private String tenantId;
	private String regionId;
	private String userId;
	private Map<String, Double> eventTypeAggregate;
	private double dayScore;
	private Date date;
	

	public DayScore(String tenantId, String regionId, long timestamp, String movieId,String userId,
			Map<String, Double> eventTypeAggregate, double dayScore, Date date) {
	
		this.itemId = movieId;
		this.timestamp = timestamp;
		this.tenantId = tenantId;
		this.regionId = regionId;
		this.userId = userId;
		this.eventTypeAggregate = eventTypeAggregate;
		this.dayScore = dayScore;
		this.date = date;
	}

	public DayScore() {
		super();
	}

	public double getDayScore() {
		return dayScore;
	}
	
	public Date getDate() {
		return date;
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
	
	public void setUserId(String userId) {
		this.userId = userId;
	}

	public void setDayScore(double dayScore) {
		this.dayScore = dayScore;
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

	public String getUserId() {
		return userId;
	}
	
	public void setDate(Date date) {
		this.date = date;
	}
	
	public Map<String, Double> getEventTypeAggregate() {
		return eventTypeAggregate;
	}

	public void setEventTypeAggregate(Map<String, Double> eventTypeAggregate) {
		this.eventTypeAggregate = eventTypeAggregate;
	}

	@Override
	public String toString() {
		return getTimestamp()+DELIM+getItemId()+DELIM+getTenantId()+DELIM+getRegionId()+DELIM+getUserId()+DELIM+getDayScore()
				+DELIM+getDate()+DELIM+getEventTypeAggregate();
	}
}
