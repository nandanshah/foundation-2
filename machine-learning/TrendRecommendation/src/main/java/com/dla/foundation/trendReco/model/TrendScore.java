package com.dla.foundation.trendReco.model;

import java.io.Serializable;

public class TrendScore implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4236461259788692600L;
	long timestamp;
	String tenantId;
	String regionId;
	String itemId;

	double trendScore;
	double normalizedScore;

	public TrendScore(String tenantId, String regionId, String itemId, double trend,
			long timestamp) {
		super();
		this.tenantId = tenantId;
		this.regionId = regionId;
		this.itemId = itemId;
		this.trendScore = trend;
		this.timestamp = timestamp;
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

	public String getItemId() {
		return itemId;
	}

	public double getTrendScore() {
		return trendScore;
	}

	public void setTrendScore(double trendScore) {
		this.trendScore = trendScore;
	}

	public void setNormalizedScore(double normalizedScore) {
		this.normalizedScore = normalizedScore;
	}

	public double getNormalizedScore() {
		return normalizedScore;
	}

}
