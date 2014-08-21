package com.dla.foundation.useritemreco.model;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public class ItemSummary implements Serializable {

	private static final long serialVersionUID = 4399190419319538112L;
	private String tenantId;
	private String regionId;
	private String itemId;
	private Map<String, Score> scores;
	private Date date;

	public String getItemId() {
		return itemId;
	}

	public Map<String, Score> getScores() {
		return scores;
	}

	public Date getDate() {
		return date;
	}

	public String getTenantId() {
		return tenantId;
	}

	public String getRegionId() {
		return regionId;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
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

	public void setScores(Map<String, Score> scores) {
		this.scores = scores;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public ItemSummary(String tenantId, String regionId, String itemId,
			Map<String, Score> scores, Date date) {
		super();
		this.tenantId = tenantId;
		this.regionId = regionId;
		this.itemId = itemId;
		this.scores = scores;
		this.date = date;
	}

}
