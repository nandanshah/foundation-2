package com.dla.foundation.fis.eo.dispatcher;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.dla.foundation.fis.eo.entities.*;
import com.dla.foundation.fis.eo.exception.DispatcherException;

/**
 * This interface will provide the methods open for web service developer
 * 
 * @author shishir_shivhare
 * 
 */
public interface EODispatcher extends Closeable {

	public void init(EOConfig eoConfig) throws IOException;

	public boolean profileAdded(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

	public boolean profileDeleted(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean profileUpdatePreferredRegion(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId,
			UUID preferredregionId) throws DispatcherException;

	public boolean profileUpdatePreferredLocale(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId,
			UUID preferredLocaleId) throws DispatcherException;

	public boolean profileUpdateNewSocialMediaAccountLinkage(UUID tenantId,
			UUID sessionId, UUID accountId, long timestamp, UUID profileId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId,
			SocialMediaType socialMediaType, String gigyaAuthToken)
					throws DispatcherException;

	public boolean profileUpdateSocialMediaAccountDelete(UUID tenantId,
			UUID sessionId, UUID accountId, long timestamp, UUID profileId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId,
			SocialMediaType socialMediaType, String gigyaAuthToken)
					throws DispatcherException;

	public boolean accountAdd(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

	public boolean accountDelete(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

	public boolean accountInfoUpdate(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

	public boolean userLogin(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

	public boolean userLogout(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

	public boolean addSession(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, Map<String, String> avp,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean userSearch(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, SearchType searchType,
			String searchString, Map<String, String> filters, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean userSearchResultClick(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId,
			SearchType searchType, String searchString, int resultPageNumber,
			UUID itemId, int rankOfItemId, UserActions action, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean userItemPreview(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean userItemMoreInfo(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean userItemShare(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean userItemRate(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, int rateScore,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean userItemAddToWatchList(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean userItemDeleteFromWatchlist(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean userItemPlayStart(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean userItemPlayPercentage(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

	public boolean userItemPlayStop(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

	public boolean userItemPlayPause(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

	public boolean userItemPlayResume(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

	public boolean userItemRent(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			long rentStartTimestamp, long rentEndTimestamp, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean userItemPurchase(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			long purchaseStartTimestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

	public boolean userItemImpression(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	// methods with optional attribute
	public boolean accountInfoUpdate(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, Map<String, String> avp,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException;

	public boolean userItemPreview(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException;

	public boolean userItemMoreInfo(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException;

	public boolean userItemShare(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException;

	public boolean userItemRate(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, int rateScore,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException;

	public boolean userItemPlayStart(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException;

	public boolean userItemPlayPercentage(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException;

	public boolean userItemPlayStop(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException;

	public boolean userItemPlayPause(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException;

	public boolean userItemPlayResume(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException;

	public boolean userItemRent(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			long rentStartTimestamp, long rentEndTimestamp,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException;

	public boolean userItemAddToWatchList(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

	public boolean userItemPurchase(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			long purchaseStartTimestamp, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException;

	public boolean userItemImpression(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException;

}
