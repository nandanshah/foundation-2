package com.dla.foundation.trendReco.model;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public class UserEvent implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -808961715382376700L;

	int userId;
	int movieId;
	int eventType;
	Date date;
	int tenantId;
	int regionId;
	Map<String, String> avp;

	public int getMovieid() {
		return movieId;
	}

	public void setMovieid(int movieid) {
		this.movieId = movieid;
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public int getEventType() {
		return eventType;
	}

	public void setEventType(int event_type) {
		this.eventType = event_type;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public int getTenantId() {
		return tenantId;
	}

	public void setTenantId(int tenantID) {
		this.tenantId = tenantID;
	}

	public int getRegionId() {
		return regionId;
	}

	public void setRegionId(int regionId) {
		this.regionId = regionId;
	}

	public Map<String, String> getAvp() {
		return avp;
	}

	public void setAvp(Map<String, String> avp) {

		this.avp = avp;
	}

	@Override
	public String toString() {

		return regionId + "::" + tenantId + "::" + movieId + avp.keySet();
	}
}
