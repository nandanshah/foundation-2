package com.dla.foundation.fis.eo.entities;

public enum UserActions {
	
	ITEM_PREVIEW("item_preview"),
	ITEM_MORE_INFO("item_more_info"),
	ITEM_SHARE("item_share"),
	ITEM_RATE("item_rate"),
	ITEM_PLAY("item_play"),
	ITEM_PLAY_PAUSE("item_play_pause"),
	ITEM_PLAY_RESUME("item_play_resume"),
	ITEM_PLAY_STOP("item_play_stop"),
	ITEM_ADD_TO_WATCHLIIST("item_add_to_watchlist"),
	ITEM_REMOVE_FROM_WATCHLIST("item_remove_from_watchlist"),
	ITEM_RENT("item_rent"),
	ITEM_PURCHASE("item_purchase");
	
	private String userActions;

	private UserActions(String userActions) {
		this.userActions = userActions;
	}

	public String getUserActions() {
		return userActions;
	}

	
}
