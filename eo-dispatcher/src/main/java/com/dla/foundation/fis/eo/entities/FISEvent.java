package com.dla.foundation.fis.eo.entities;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

public class FISEvent implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -170887874513556821L;
	
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
	
	// Basic Required Properties
	public String analyticsVersion; // Version. **REQUIRED**	
	public String visitorProfileId; // Profile ID. **REQUIRED**
	public String hitType; // Hit type. **REQUIRED**
	public String hitDateTime; // Event date/time stamp. **REQUIRED**

	// Visitor
	public String visitorDeviceId; // Device ID.

	// Session
	public String sessionControl; // A value of 'start' forces a new session to start with this hit and 'end' forces the current session to end with this hit. All other values are ignored.

	// Traffic Sources
	public String documentReferrer; // Referring URL.
	public String campaignName; // Campaign name.
	public String campaignSource; // Campaign source.
	public String campaignMedium; // Campaign medium.
	public String campaignKeyword; // Campaign keyword.
	public String campaignContent; // Campaign content.
	public String campaignId; // Campaign ID.
	public String adWordsId; // AdWords ID.
	public String displayAdsId; // Display Ads ID.

	// System Info
	public String screenResolution; // Screen Resolution. (ex. 800x600)
	public String viewportSize; // Viewable area of the browser / device. (ex. 123x456)
	public String documentEncoding; // Character set used to encode the page / document. (ex. UTF-8)
	public String screenColorDepth; // Screen color depth. (ex. 24-bits)
	public String languageCode; // System language code. (ex. en-us)
	public Boolean javaEnabled; // Is Java enabled?
	public String region;
	public String tennant;
	public String geoCoordinates; // Latitude-Longitude coordinates
	public String ipAddress;
	public String foundationSessionId;

	// Hit Data
	public Boolean isNonInteractionHit; // Should a hit be considered non-interactive?

	// Content Information
	public String documentLocation; // Full URL (document location) of the page on which content resides.
	public String documentHostName; // Document hostname.
	public String documentPath; // Path portion of the page URL. Should begin with '/'.
	public String documentTitle; // Title of the page/document.
	public String contentDescription; // If not specified, this will default to the unique URL of the page by either using the documentLocation parameter as-is or assembling it from documentHostName and documentPath. App tracking makes use of this for the 'Screen Name' of the appview hit.
	public String linkId; // ID of a clicked DOM element.

	// App Tracking
	public String appName; // App name.
	public String appVersion; // App version.

	// Event Tracking		
	public String customEventCategory; // Event Category.
	public String customEventAction; // Event Action.
	public String customEventLabel; // Event label.
	public String customEventValue; // Event value.

	// E-Commerce Tracking
	public String transactionId; // Transaction ID.
	public String transactionCurrency;	// Transaction Currency code.
	public String transactionAffiliation; // Transaction affiliation.
	public BigDecimal transactionRevenue; // Transaction revenue.
	public BigDecimal transactionShipping; // Transaction shipping.
	public BigDecimal transactionTax; // Transaction tax.
	public String itemName; // Item name. Required.
	public BigDecimal itemPrice; // Item price.
	public BigDecimal itemQuantity; // Item quantity.
	public String itemCode; // Item code / SKU.
	public String itemVariation; // Item variation / category.

	// Social Interactions
	public String socialAction; // Social Action.
	public String socialNetwork; // Social Network.
	public String socialTarget; // Target of a social interaction. This value is typically a URL but can be any text.

	// Exception Interactions
	public String exceptionDescription; // Exception description.
	public Boolean exceptionIsFatal; // Exception is fatal?

	// Timing Interactions
	public String timingCategory; // Timing category.
	public String timingVariable; // Timing variable.
	public Integer timingTime; // Timing time in milliseconds.
	public String timingLabel; // Timing label.
	public Integer timingPageLoadTime; // Page load time in milliseconds.
	public Integer timingDnsLoad; // DNS load time in milliseconds.
	public Integer timingDownload; // Page download time in milliseconds.
	public Integer timingRedirect; // Redirect response time in milliseconds.
	public Integer timingTcpConnect; // TCP connect time in milliseconds.
	public Integer timingServerResponse; // Server response time in milliseconds.
	public Integer queueTime; // Latency time between an offline hit event and online reported time in milliseconds

	// Custom Metrics
	public String customDimension; // Custom dimension by index (ex. customDimension[1-9][0-9]*=Sports)
	public String customMetric; // Custom metric by index (ex. customMetric[1-9][0-9]*=47)

	// Content Experiments
	public String experimentId; // Experiment ID.
	public String experimentVariant; // Experiment variant;

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
	public static FISEvent fromBytes(byte[] body) {
		FISEvent obj = null;
		try (ByteArrayInputStream bis = new ByteArrayInputStream(body)) {
			try (ObjectInputStream ois = new ObjectInputStream(bis)) {
				obj = (FISEvent) ois.readObject();
			}
		}
		catch (Exception e) {
			return null;
		}
		return obj;
	}
}
