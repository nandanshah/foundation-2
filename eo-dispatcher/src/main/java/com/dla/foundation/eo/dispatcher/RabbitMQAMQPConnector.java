package com.dla.foundation.eo.dispatcher;

import java.util.UUID;

import com.dla.foundation.eo.entities.DeviceType;
import com.dla.foundation.eo.entities.ImpressionSource;
import com.dla.foundation.eo.entities.NetworkType;
import com.dla.foundation.eo.entities.SearchType;
import com.dla.foundation.eo.entities.SocialMediaType;

/**
 * This class will call rabbitmq push event depending upon the method called with event type(act as enum). 
 * @author shishir_shivhare
 *
 */
public class RabbitMQAMQPConnector extends AbstractRMQDispatcher {

	
	@Override
	public boolean profileAdded(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID preferredRegionId,
			UUID preferredLocaleId) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean profileDeleted(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean profileUpdatePreferredRegion(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId,
			UUID preferredRegionId) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean profileUpdatePreferredLocale(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId,
			UUID preferredLocaleId) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean profileUpdateNewSocialMediaAccountLinkage(UUID tenantId,
			UUID sessionId, UUID accountId, long timestamp, UUID profileId,
			SocialMediaType socialMediaType, String gigyaAuthToken) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean profileUpdateSocialMediaAccountDelete(UUID tenantId,
			UUID sessionId, UUID accountId, long timestamp, UUID profileId,
			SocialMediaType socialMediaType, String gigyaAuthToken) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean accountAdd(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean accountDelete(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean accountInfoUpdate(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean userLogin(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userLogout(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userSearch(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, SearchType searchType,
			String searchString) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemPreview(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemMoreInfo(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemShare(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemRate(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, double rateScore) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemAddToWatchList(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemDeleteFromWatchlist(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemPlayStart(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemPlayPercentage(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemPlayStop(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemPlayPause(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemPlayResume(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemRent(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			long rent_start_timestamp, long rent_end_timestamp) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemPurchase(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			long purchaseStartTimestamp) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userLogin(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, DeviceType deviceType,
			UUID regionId, UUID localeId, NetworkType networkType) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemPreview(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, NetworkType networkType) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemMoreInfo(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, NetworkType networkType) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemShare(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, NetworkType networkType) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemRate(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, double rateScore,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, NetworkType networkType) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemPlayStart(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, NetworkType networkType) {
		// TODO Auto-generated method stub
		return true;

	}

	@Override
	public boolean userItemPlayPercentage(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId,
			NetworkType networkType) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean userItemPlayStop(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId,
			NetworkType networkType) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean userItemPlayPause(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId,
			NetworkType networkType) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean userItemPlayResume(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId,
			NetworkType networkType) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean userItemRent(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			long rent_start_timestamp, long rent_end_timestamp,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, NetworkType networkType) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean userItemPurchase(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			long purchaseStartTimestamp, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId,
			NetworkType networkType) {
		// TODO Auto-generated method stub
		return true;
	}
}
