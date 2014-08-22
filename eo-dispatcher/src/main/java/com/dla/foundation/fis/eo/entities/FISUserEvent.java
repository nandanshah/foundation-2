package com.dla.foundation.fis.eo.entities;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/**
 * Represents events used by Foundation Intelligence System
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class FISUserEvent implements Serializable {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3491500224917042664L;
	
	// Properties used by Foundation intelligence system.
	public EventType type;
	public UUID tenantID;
	public UUID sessionID;
	public UUID accountID;
	public long timestamp;
	public UUID profileID;
	public UUID regionID;
	public UUID preferredRegionID;
	public UUID localeID;
	public UUID deviceId;
	public UUID preferredLocaleID;
	public SocialMediaType socialMediaType;
	public String gigyaAuthToken;
	public UUID itemID;
	public ImpressionSource impressionSource;
	public DeviceType deviceType;
	public NetworkType networkType;//
	public double playPercentage;
	public long purchaseTimestamp;//
	public int rateScore;
	public long rentStartTimestamp;
	public long rentEndTimestamp;
	public SearchType searchType;
	public String searchQuery;
	public Map<String, String> avp;
	public Map<String, String> filters;
	public int resultPageNumber;
	public int rankOfItemId;
	public UserActions action;
	public long purchaseStartTimestamp;

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
	public static FISUserEvent fromBytes(byte[] body) {
		FISUserEvent obj = null;
		try (ByteArrayInputStream bis = new ByteArrayInputStream(body)) {
			try (ObjectInputStream ois = new ObjectInputStream(bis)) {
				obj = (FISUserEvent) ois.readObject();
			}
		}
		catch (Exception e) {
			return null;
		}
		return obj;
	}
}
