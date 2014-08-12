package com.dla.foundation.fis.eo.entities;

public enum EventType {

	profileAdded("profileAdded"), profileDeleted("profileDeleted"), profileUpdatePreferredRegion(
			"profileUpdatePreferredRegion"), profileUpdatePreferredLocale(
			"profileUpdatePreferredLocale"), profileUpdateNewSocialMediaAccountLinkage(
			"profileUpdateNewSocialMediaAccountLinkage"), profileUpdateSocialMediaAccountDelete(
			"profileUpdateSocialMediaAccountDelete"), accountAdd("accountAdd"), accountDelete(
			"accountDelete"), accountInfoUpdate("accountInfoUpdate"), userLogin(
			"userLogin"), userLogout("userLogout"), addSession("addSession"), userSearch(
			"userSearch"), userSearchResultClick("userSearchResultClick"), userItemPreview(
			"userItemPreview"), userItemMoreInfo("userItemMoreInfo"), userItemShare(
			"userItemShare"), userItemRate("userItemRate"), userItemAddToWatchList(
			"userItemAddToWatchList"), userItemDeleteFromWatchlist(
			"userItemDeleteFromWatchlist"), userItemPlayStart(
			"userItemPlayStart"), userItemPlayPercentage(
			"userItemPlayPercentage"), userItemPlayStop("userItemPlayStop"), userItemPlayPause(
			"userItemPlayPause"), userItemPlayResume("userItemPlayResume"), userItemRent(
			"userItemRent"), userItemPurchase("userItemPurchase"), userItemImpression(
			"userItemImpression");

	private String eventType;

	private EventType(String eventType) {
		this.eventType = eventType;
	}

	public String getEventType() {
		return eventType;
	}
}
