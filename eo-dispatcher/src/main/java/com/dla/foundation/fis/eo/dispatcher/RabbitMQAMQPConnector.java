package com.dla.foundation.fis.eo.dispatcher;

import java.util.Map;
import java.util.UUID;

import com.dla.foundation.fis.eo.entities.AnalyticsEvent;
import com.dla.foundation.fis.eo.entities.DeviceType;
import com.dla.foundation.fis.eo.entities.EOConfig;
import com.dla.foundation.fis.eo.entities.ImpressionSource;
import com.dla.foundation.fis.eo.entities.NetworkType;
import com.dla.foundation.fis.eo.entities.SearchType;
import com.dla.foundation.fis.eo.entities.SocialMediaType;
import com.dla.foundation.fis.eo.entities.UserActions;
import com.dla.foundation.fis.eo.exception.DispatcherException;

/**
 * This class will call rabbitmq push event depending upon the method called with event type(act as enum). 
 * @author shishir_shivhare
 *
 */
public class RabbitMQAMQPConnector extends AbstractRMQDispatcher<AnalyticsEvent> {

	public RabbitMQAMQPConnector() {
		this.conf = new EOConfig();
	}

	@Override
	public boolean profileAdded(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean profileDeleted(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean profileUpdatePreferredRegion(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId,
			UUID preferredregionId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean profileUpdatePreferredLocale(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId,
			UUID preferredLocaleId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean profileUpdateNewSocialMediaAccountLinkage(UUID tenantId,
			UUID sessionId, UUID accountId, long timestamp, UUID profileId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId,
			SocialMediaType socialMediaType, String gigyaAuthToken)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean profileUpdateSocialMediaAccountDelete(UUID tenantId,
			UUID sessionId, UUID accountId, long timestamp, UUID profileId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId,
			SocialMediaType socialMediaType, String gigyaAuthToken)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean accountAdd(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		AnalyticsEvent e = new AnalyticsEvent();
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		enqueueAsync(e, RabbitMQConnectorConstants.accountAdd_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean accountDelete(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean accountInfoUpdate(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userLogin(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userLogout(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addSession(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, Map<String, String> avp,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userSearch(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, SearchType searchType,
			String searchString, Map<String, String> filters, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userSearchResultClick(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId,
			SearchType searchType, String searchString, int resultPageNumber,
			UUID itemId, int rankOfItemId, UserActions action, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPreview(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemMoreInfo(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemShare(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemRate(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, int rateScore,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemAddToWatchList(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemDeleteFromWatchlist(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPlayStart(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPlayPercentage(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPlayStop(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPlayPause(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPlayResume(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemRent(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			long rentStartTimestamp, long rentEndTimestamp, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPurchase(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			long purchaseStartTimestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemImpression(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean accountInfoUpdate(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, Map<String, String> avp,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPreview(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemMoreInfo(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemShare(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemRate(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, int rateScore,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPlayStart(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPlayPercentage(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPlayStop(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPlayPause(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPlayResume(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemRent(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			long rentStartTimestamp, long rentEndTimestamp,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemAddToWatchList(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemPurchase(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			long purchaseStartTimestamp, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
			throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userItemImpression(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		// TODO Auto-generated method stub
		return false;
	}

	
}
