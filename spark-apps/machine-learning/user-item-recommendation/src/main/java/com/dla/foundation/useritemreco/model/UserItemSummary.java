package com.dla.foundation.useritemreco.model;

import java.io.Serializable;

public class UserItemSummary implements Serializable {

	private static final long serialVersionUID = -828822672311537746L;

	private String userId;
	private ItemSummary itemSummary;

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public UserItemSummary(String userId, ItemSummary itemSummary) {
		super();
		this.userId = userId;
		this.itemSummary = itemSummary;
	}

	public UserItemSummary() {

	}

	public void setItemSummary(ItemSummary itemSummary) {
		this.itemSummary = itemSummary;
	}

	public ItemSummary getItemSummary() {
		return itemSummary;
	}

}
