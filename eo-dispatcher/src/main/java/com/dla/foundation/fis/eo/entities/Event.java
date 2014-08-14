package com.dla.foundation.fis.eo.entities;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents events used by Foundation Intelligence System
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class Event implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2249381207159823917L;
	
	public String trackingPropertyId; // Tracking ID / Client property / Property ID. **REQUIRED**
	public String analyticsVersion; // Version. **REQUIRED**
	public String visitorProfileId; // Profile ID. **REQUIRED**
	public String hitType; // Hit type. **REQUIRED**
	public String hitDateTime; // Event date/time stamp. **REQUIRED**
	public String accountId;
	
	public String visitorDeviceId; // Device ID.
	public String visitorDeviceType; // Device Type.
	
	public String sessionControl; // A value of 'start' forces a new session to start with this hit and 'end' forces the current session to end with this hit. All other values are ignored.
	
	public String documentReferrer; // Referring URL.
	public String campaignName; // Campaign name.
	public String campaignSource; // Campaign source.
	public String campaignMedium; // Campaign medium.
	public String campaignKeyword; // Campaign keyword.
	public String campaignContent; // Campaign content.
	public String campaignId; // Campaign ID.
	public String adWordsId; // Google AdWords ID.
	public String displayAdsId; // Google Display Ads ID.

	public String screenResolution; // Screen Resolution. (ex. 800x600)
	public String viewportSize; // Viewable area of the browser / device. (ex. 123x456)
	public String documentEncoding; // Character set used to encode the page / document. (ex. UTF-8)
	public String screenColorDepth; // Screen color depth. (ex. 24-bits)
	public String userLanguage; // System language code. (ex. en-us)
	public Boolean javaEnabled; // Is Java enabled?
	public String regionId;
	public String tennantId;
	public String geoCoordinates; // Latitude-Longitude coordinates
	public String ipAddress;
	public String foundationSessionId;
	
	public Boolean isNonInteractionHit; // Should a hit be considered non-interactive?
	public String documentLocation; // Full URL (document location) of the page on which content resides.
	public String documentHostName; // Document hostname.
	public String documentPath; // Path portion of the page URL. Should begin with '/'.
	public String documentTitle; // Title of the page/document.
	public String contentDescription; // If not specified, this will default to the unique URL of the page by either using the documentLocation parameter as-is or assembling it from documentHostName and documentPath. App tracking makes use of this for the 'Screen Name' of the appview hit.
	public String linkId; // ID of a clicked DOM element.
	public String appName; // App name.
	public String appVersion; // App version.
	public String transactionId; // Transaction ID.
	public String transactionCurrency;	// Transaction Currency code.
	public String transactionAffiliation; // Transaction affiliation.
	public String transactionRevenue; // Transaction revenue.
	public String transactionShipping; // Transaction shipping.
	public String transactionTax; // Transaction tax.
	
	public String itemId;
	public String itemName; // Item name. Required.
	public String itemPrice; // Item price.
	public String itemQuantity; // Item quantity.
	public String itemCode; // Item code / SKU.
	public String itemVariation; // Item variation / category.
	
	public String customEventCategory; // Event Category.
	public String customEventAction; // Event Action.
	public String customEventLabel; // Event label.
	public String customEventValue; // Event value.

	public String socialAction; // Social Action.
	public String socialNetwork; // Social Network.
	public String socialTarget; // Target of a social interaction. This value is typically a URL but can be any text.

	public String exceptionDescription; // Exception description.
	public Boolean exceptionIsFatal; // Exception is fatal?

	public String timingCategory; // Timing category.
	public String timingVariable; // Timing variable.
	public String timingTime; // Timing time in milliseconds.
	public String timingLabel; // Timing label.
	public String timingPageLoadTime; // Page load time in milliseconds.
	public String timingDnsLoad; // DNS load time in milliseconds.
	public String timingDownload; // Page download time in milliseconds.
	public String timingRedirect; // Redirect response time in milliseconds.
	public String timingTcpConnect; // TCP connect time in milliseconds.
	public String timingServerResponse; // Server response time in milliseconds.
	public String queueTime; // Latency time between an offline hit event and online reported time

	public String customDimension; // Custom dimension by index (ex. customDimension[1-9][0-9]*=Sports)
	public String customMetric; // Custom metric by index (ex. customMetric[1-9][0-9]*=47)

	public String experimentId; // Experiment ID.
	public String experimentVariant; // Experiment variant;
	
	public String networkType;
	public String city;
	public String userPreferredLocale;
	public String userCurrentLocale;
	public String itemLocale;
	
	//TODO
	//public String entitlement 
	
	public String eventContext;
	public String view;
	public String authType;
	public String clientId;
	public String clientVersion;
	public String playPercentage;
	public String contentLengthInMins;
	public String rentStart;
	public String rentEnd;
	public String licenseId;
	public String service;
	public String serviceVersion;
	public String qualityOfContent; //HD/SD
	public String qualityOfPlayback; //HD/SD
	public String originationTimestamp;
	public String recievedTimestamp;
	
	//TODO
	//public String QoS;
	
	public Map<String,String> addlParams = new HashMap<>();

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
	public static Event fromBytes(byte[] body) {
		Event obj = null;
		try (ByteArrayInputStream bis = new ByteArrayInputStream(body)) {
			try (ObjectInputStream ois = new ObjectInputStream(bis)) {
				obj = (Event) ois.readObject();
			}
		}
		catch (Exception e) {
			return null;
		}
		return obj;
	}
	
}
