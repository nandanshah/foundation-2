package com.dla.foundation.intelligence.entity;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.persistence.Entity;

import com.dla.foundation.data.annotation.Index;
import com.dla.foundation.data.entities.Device.DeviceType;
import com.dla.foundation.data.entities.event.Event;
import com.dla.foundation.data.entities.event.EventContextType;
import com.dla.foundation.data.entities.event.EventNetworkType;
import com.dla.foundation.data.entities.event.EventType;
import com.dla.foundation.data.entities.event.SearchType;
import com.dla.foundation.data.entities.event.UserActions;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;
import com.dla.foundation.data.entities.social.SocialProvider;

/**
 * Represents events used by Foundation Intelligence System
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
@Entity
public class UserEvent extends SimpleFoundationEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7393607575768341688L;

	// Properties used by Foundation intelligence system.
	public UUID tenantID;
	public UUID regionID;
	public UUID profileID;
	public UUID itemID;
	public UUID accountID;
	public UUID localeID;
	public UUID deviceId;
	public UUID sessionID;
	public EventType eventType;
	public long timestamp;
	public DeviceType deviceType;
	public SearchType searchType;
	public String searchString;
	public int resultPageNumber;
	public UserActions useraction;
	public int rankOfItem;
	public EventContextType sourceOfImpression;
	public double playPercentage;
	public long rentStartTimestamp;
	public long rentEndTimestamp;
	public long purchaseStartTimestamp;
	public Map<String, String> attributeValuePair;
	@Index
	public int eventrequired;
	@Index
	public Date date;
	public int rateScore;
	public Map<String, String> searchFilters;

	public UUID preferredRegionID;
	public UUID preferredLocaleID;
	public SocialProvider socialMediaType;
	public String gigyaAuthToken;
	public EventNetworkType networkType;
	public long purchaseTimestamp;
	
	public Map<String,String> addlParams = new HashMap<>();

	//getBytes() will allow event orchestration layer broadcaster to serialize analytics events
	public byte[] toBytes() {
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
	public static UserEvent fromBytes(byte[] body) {
		UserEvent obj = null;
		try (ByteArrayInputStream bis = new ByteArrayInputStream(body)) {
			try (ObjectInputStream ois = new ObjectInputStream(bis)) {
				obj = (UserEvent) ois.readObject();
			}
		}
		catch (Exception e) {
			return null;
		}
		return obj;
	}

	public static UserEvent copy(Event ue) {
		UserEvent userEvent = new UserEvent();
		userEvent.accountID = strToUUID(ue.accountId);
		userEvent.deviceId = strToUUID(ue.visitorDeviceId);
		userEvent.deviceType = (ue.visitorDeviceType!=null) ? DeviceType.valueOf(ue.visitorDeviceType) : null;
		userEvent.sourceOfImpression = ue.eventContext;
		userEvent.itemID = strToUUID(ue.itemId);
		userEvent.localeID = strToUUID(ue.userCurrentLocale);
		userEvent.networkType = ue.networkType;
		userEvent.playPercentage = strToDouble(ue.playPercentage);
		userEvent.preferredLocaleID = strToUUID(ue.userPreferredLocale);
		userEvent.preferredRegionID = strToUUID(ue.homeRegionId);
		userEvent.profileID = strToUUID(ue.visitorProfileId);
		userEvent.purchaseStartTimestamp = strToLong(ue.startTimestamp);
		userEvent.purchaseTimestamp = strToLong(ue.startTimestamp);
		userEvent.rateScore = strToInt(ue.rateScore);
		userEvent.regionID = strToUUID(ue.currentRegionId);
		userEvent.rentEndTimestamp = strToLong(ue.endTimestamp);
		userEvent.rentStartTimestamp = strToLong(ue.startTimestamp);
		//userEvent.useraction = ue.action;
		//userEvent.rankOfItem = ue.rankOfItemId;
		//userEvent.resultPageNumber = ue.resultPageNumber;
		//userEvent.attributeValuePair = ue.attributeValuePair;
		//userEvent.searchFilters = ue.filters;
		userEvent.searchString = ue.userInputData;
		userEvent.searchType = ue.searchType;
		userEvent.sessionID = strToUUID(ue.foundationSessionId);
		userEvent.socialMediaType = ue.socialProvider;
		userEvent.tenantID = strToUUID(ue.tenantId);
		userEvent.timestamp = strToLong(ue.originationTimestamp);
		userEvent.eventType = ue.eventType;
		if(ue.addlParams!=null && ue.addlParams.containsKey("gigyaAuthToken")) {
			userEvent.gigyaAuthToken = ue.addlParams.get("gigyaAuthToken");
		}
		userEvent.addlParams = ue.addlParams;
		return userEvent;
	}
	
	private static UUID strToUUID(String strId) {
		return  (strId != null) ? UUID.fromString(strId) : null; 
	}
	
	private static Double strToDouble(String strId) {
		return  (strId != null) ? Double.parseDouble(strId) : 0.0; 
	}
	
	private static Long strToLong(String strId) {
		return  (strId != null) ? Long.parseLong(strId) : 0; 
	}
	
	private static Integer strToInt(String strId) {
		return  (strId != null) ? Integer.parseInt(strId) : -1; 
	}
}