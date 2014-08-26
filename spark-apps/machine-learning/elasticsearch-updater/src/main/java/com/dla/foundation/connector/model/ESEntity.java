package com.dla.foundation.connector.model;

import java.io.Serializable;
import java.util.Date;

public class ESEntity implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8177942265132177090L;
	public String id;
	public String regionId;
	Date date;
	public String tenantId;
	
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTenantId() {
		return tenantId;
	}
	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}
	public String getRegionId() {
		return regionId;
	}
	public void setRegionId(String regionId) {
		this.regionId = regionId;
	}
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	} 
}
