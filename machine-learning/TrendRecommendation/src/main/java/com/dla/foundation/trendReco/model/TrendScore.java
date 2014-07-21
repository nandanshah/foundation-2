package com.dla.foundation.trendReco.model;

import java.io.Serializable;

public class TrendScore implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4236461259788692600L;
	long timestamp;
	int tenantId;
	int regionId;
	int itemId;

	double trendScore;
	double normalizedScore;

	public TrendScore(int tenantId, int regionId, int itemId, double trend,
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

	public int getTenantId() {
		return tenantId;
	}

	public int getRegionId() {
		return regionId;
	}

	public int getItemId() {
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
