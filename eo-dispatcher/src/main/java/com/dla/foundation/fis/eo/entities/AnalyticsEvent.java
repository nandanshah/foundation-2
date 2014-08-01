package com.dla.foundation.fis.eo.entities;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

public class AnalyticsEvent implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1170682511448219134L;

	private UUID tenantID;
	private UUID sessionID;
	private UUID accountID;
	private long timestamp;
	private UUID profileID;
	private UUID regionID;
	private UUID preferredRegionID;
	private UUID localeID;
	private UUID deviceId;
	private UUID preferredLocaleID;
	private SocialMediaType socialMediaType;
	private String gigyaAuthToken;
	private UUID itemID;
	private ImpressionSource impressionSource;
	private DeviceType deviceType;
	private NetworkType networkType;
	private double playPercentage;
	private long purchaseTimestamp;
	private int rateScore;
	private long rentStartTimestamp;
	private long rentEndTimestamp;
	private SearchType searchType;
	private String searchQuery;
	private Map<String, String> avp;
	private Map<String, String> filters;
	private int resultPageNumber;
	private int rankOfItemId;
	private UserActions action;
	private long purchaseStartTimestamp;
		
	public AnalyticsEvent() {
		
	}
	
	public UUID getTenantID() {
		return tenantID;
	}

	public void setTenantID(UUID tenantID) {
		this.tenantID = tenantID;
	}

	public UUID getSessionID() {
		return sessionID;
	}

	public void setSessionID(UUID sessionID) {
		this.sessionID = sessionID;
	}

	public UUID getAccountID() {
		return accountID;
	}

	public void setAccountID(UUID accountID) {
		this.accountID = accountID;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public UUID getProfileID() {
		return profileID;
	}

	public void setProfileID(UUID profileID) {
		this.profileID = profileID;
	}

	public UUID getPreferredRegionID() {
		return preferredRegionID;
	}

	public void setPreferredRegionID(UUID preferredRegionID) {
		this.preferredRegionID = preferredRegionID;
	}

	public UUID getPreferredLocaleID() {
		return preferredLocaleID;
	}

	public void setPreferredLocaleID(UUID preferredLocaleID) {
		this.preferredLocaleID = preferredLocaleID;
	}

	public SocialMediaType getSocialMediaType() {
		return socialMediaType;
	}

	public void setSocialMediaType(SocialMediaType socialMediaType) {
		this.socialMediaType = socialMediaType;
	}

	public String getGigyaAuthToken() {
		return gigyaAuthToken;
	}

	public void setGigyaAuthToken(String gigyaAuthToken) {
		this.gigyaAuthToken = gigyaAuthToken;
	}

	public UUID getItemID() {
		return itemID;
	}

	public void setItemID(UUID itemID) {
		this.itemID = itemID;
	}

	public ImpressionSource getImpressionSource() {
		return impressionSource;
	}

	public void setImpressionSource(ImpressionSource impressionSource) {
		this.impressionSource = impressionSource;
	}

	public DeviceType getDeviceType() {
		return deviceType;
	}

	public void setDeviceType(DeviceType deviceType) {
		this.deviceType = deviceType;
	}

	public UUID getRegionID() {
		return regionID;
	}

	public void setRegionID(UUID regionID) {
		this.regionID = regionID;
	}

	public UUID getLocaleID() {
		return localeID;
	}

	public void setLocaleID(UUID localeID) {
		this.localeID = localeID;
	}

	public NetworkType getNetworkType() {
		return networkType;
	}

	public void setNetworkType(NetworkType networkType) {
		this.networkType = networkType;
	}

	public double getPlayPercentage() {
		return playPercentage;
	}

	public void setPlayPercentage(double playPercentage) {
		this.playPercentage = playPercentage;
	}

	public long getPurchaseTimestamp() {
		return purchaseTimestamp;
	}

	public void setPurchaseTimestamp(long purchaseTimestamp) {
		this.purchaseTimestamp = purchaseTimestamp;
	}

	public double getRateScore() {
		return rateScore;
	}

	public long getRentStartTimestamp() {
		return rentStartTimestamp;
	}

	public UUID getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(UUID deviceId) {
		this.deviceId = deviceId;
	}

	public Map<String, String> getAvp() {
		return avp;
	}

	public void setAvp(Map<String, String> avp) {
		this.avp = avp;
	}

	public Map<String, String> getFilters() {
		return filters;
	}

	public void setFilters(Map<String, String> filters) {
		this.filters = filters;
	}

	public int getResultPageNumber() {
		return resultPageNumber;
	}

	public void setResultPageNumber(int resultPageNumber) {
		this.resultPageNumber = resultPageNumber;
	}

	public int getRankOfItemId() {
		return rankOfItemId;
	}

	public void setRankOfItemId(int rankOfItemId) {
		this.rankOfItemId = rankOfItemId;
	}

	public UserActions getAction() {
		return action;
	}

	public void setAction(UserActions action) {
		this.action = action;
	}

	public long getPurchaseStartTimestamp() {
		return purchaseStartTimestamp;
	}

	public void setPurchaseStartTimestamp(long purchaseStartTimestamp) {
		this.purchaseStartTimestamp = purchaseStartTimestamp;
	}

	public void setRateScore(int rateScore) {
		this.rateScore = rateScore;
	}

	public void setRentStartTimestamp(long rentStartTimestamp) {
		this.rentStartTimestamp = rentStartTimestamp;
	}

	public long getRentEndTimestamp() {
		return rentEndTimestamp;
	}

	public void setRentEndTimestamp(long rentEndTimestamp) {
		this.rentEndTimestamp = rentEndTimestamp;
	}

	public SearchType getSearchType() {
		return searchType;
	}

	public void setSearchType(SearchType searchType) {
		this.searchType = searchType;
	}

	public String getSearchQuery() {
		return searchQuery;
	}

	public void setSearchQuery(String searchQuery) {
		this.searchQuery = searchQuery;
	}

	//getBytes() will allow event orchestration layer broadcaster to serialize analytics events
	public byte[] getBytes() {
		byte[] bytes;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
				oos.writeObject(this);
				oos.flush();
				oos.reset();
				bytes = baos.toByteArray();
			}
		} catch(IOException e) {
			return null;
		}
		return bytes;
	}

	//fromBytes() will allow event orchestration layer consumer to instantiate event object from raw bytes
	public static AnalyticsEvent fromBytes(byte[] body) {
		AnalyticsEvent obj = null;
		try (ByteArrayInputStream bis = new ByteArrayInputStream(body)) {
			try (ObjectInputStream ois = new ObjectInputStream(bis)) {
				obj = (AnalyticsEvent) ois.readObject();
			}
		}
		catch (Exception e) {
			return null;
		}
		return obj;
	}
}
