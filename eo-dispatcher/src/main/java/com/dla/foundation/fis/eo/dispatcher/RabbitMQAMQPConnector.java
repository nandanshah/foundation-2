package com.dla.foundation.fis.eo.dispatcher;

import java.util.Map;
import java.util.UUID;

import com.dla.foundation.fis.eo.entities.DeviceType;
import com.dla.foundation.fis.eo.entities.EOConfig;
import com.dla.foundation.fis.eo.entities.EventType;
import com.dla.foundation.fis.eo.entities.FISEvent;
import com.dla.foundation.fis.eo.entities.ImpressionSource;
import com.dla.foundation.fis.eo.entities.SearchType;
import com.dla.foundation.fis.eo.entities.SocialMediaType;
import com.dla.foundation.fis.eo.entities.UserActions;
import com.dla.foundation.fis.eo.exception.DispatcherException;

/**
 * This class will call rabbitmq push event depending upon the method called with event type(act as enum). 
 * @author shishir_shivhare
 *
 */
public class RabbitMQAMQPConnector extends AbstractRMQDispatcher<FISEvent> {

	public RabbitMQAMQPConnector() {
		this.conf = new EOConfig();
	}

	@Override
	public boolean profileAdded(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.profileAdded);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.profileAdded_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean profileDeleted(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.profileDeleted);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.profileDeleted_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean profileUpdatePreferredRegion(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId,
			UUID preferredregionId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.profileUpdatePreferredRegion);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		e.setPreferredRegionID(preferredregionId);
		enqueueAsync(e, RabbitMQConnectorConstants.profileUpdatePreferredRegion_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean profileUpdatePreferredLocale(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId,
			UUID preferredLocaleId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.profileUpdatePreferredLocale);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		e.setPreferredLocaleID(preferredLocaleId);
		enqueueAsync(e, RabbitMQConnectorConstants.profileUpdatePreferredLocale_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean profileUpdateNewSocialMediaAccountLinkage(UUID tenantId,
			UUID sessionId, UUID accountId, long timestamp, UUID profileId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId,
			SocialMediaType socialMediaType, String gigyaAuthToken)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.profileUpdateNewSocialMediaAccountLinkage);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		e.setSocialMediaType(socialMediaType);
		e.setGigyaAuthToken(gigyaAuthToken);
		enqueueAsync(e, RabbitMQConnectorConstants.profileUpdateNewSocialMediaAccountLinkage_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean profileUpdateSocialMediaAccountDelete(UUID tenantId,
			UUID sessionId, UUID accountId, long timestamp, UUID profileId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId,
			SocialMediaType socialMediaType, String gigyaAuthToken)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.profileUpdateSocialMediaAccountDelete);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		e.setSocialMediaType(socialMediaType);
		e.setGigyaAuthToken(gigyaAuthToken);
		enqueueAsync(e, RabbitMQConnectorConstants.profileUpdateSocialMediaAccountDelete_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean accountAdd(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.accountAdd);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.accountAdd_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean accountDelete(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.accountDelete);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.accountDelete_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean accountInfoUpdate(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.accountInfoUpdate);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.accountInfoUpdate_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userLogin(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userLogin);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userLogin_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userLogout(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userLogout);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userLogout_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean addSession(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, Map<String, String> avp,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.addSession);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setAvp(avp);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.addSession_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userSearch(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, SearchType searchType,
			String searchString, Map<String, String> filters, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userSearch);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setSearchType(searchType);
		e.setSearchQuery(searchString);
		e.setFilters(filters);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userSearch_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userSearchResultClick(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId,
			SearchType searchType, String searchString, int resultPageNumber,
			UUID itemId, int rankOfItemId, UserActions action, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userSearchResultClick);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setSearchType(searchType);
		e.setSearchQuery(searchString);
		e.setResultPageNumber(resultPageNumber);
		e.setItemID(itemId);
		e.setRankOfItemId(rankOfItemId);
		e.setAction(action);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userSearchResultClick_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPreview(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPreview);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPreview_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemMoreInfo(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemMoreInfo);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemMoreInfo_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemShare(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemShare);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemShare_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemRate(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, int rateScore,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemRate);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setRateScore(rateScore);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemRate_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemAddToWatchList(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemAddToWatchList);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemAddToWatchList_WATCHLIST_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemDeleteFromWatchlist(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemDeleteFromWatchlist);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemDeleteFromWatchlist_WATCHLIST_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayStart(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPlayStart);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPlayStart_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayPercentage(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPlayPercentage);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setPlayPercentage(playPercentage);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPlayPercentage_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayStop(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPlayStop);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setPlayPercentage(playPercentage);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPlayStop_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayPause(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPlayPause);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setPlayPercentage(playPercentage);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPlayPause_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayResume(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPlayResume);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setPlayPercentage(playPercentage);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPlayResume_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemRent(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			long rentStartTimestamp, long rentEndTimestamp, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemRent);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setRentStartTimestamp(rentStartTimestamp);
		e.setRentEndTimestamp(rentEndTimestamp);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemRent_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPurchase(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			long purchaseStartTimestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPurchase);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setPurchaseStartTimestamp(purchaseStartTimestamp);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPurchase_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemImpression(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemImpression);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemImpression_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean accountInfoUpdate(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, Map<String, String> avp,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.accountInfoUpdate);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setAvp(avp);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.accountInfoUpdate_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPreview(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPreview);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setImpressionSource(impressionSource);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPreview_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemMoreInfo(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemMoreInfo);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setImpressionSource(impressionSource);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemMoreInfo_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemShare(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemShare);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setImpressionSource(impressionSource);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemShare_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemRate(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, int rateScore,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemRate);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setRateScore(rateScore);
		e.setImpressionSource(impressionSource);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemRate_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayStart(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPlayStart);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setImpressionSource(impressionSource);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPlayStart_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayPercentage(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPlayPercentage);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setPlayPercentage(playPercentage);
		e.setImpressionSource(impressionSource);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPlayPercentage_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayStop(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPlayStop);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setPlayPercentage(playPercentage);
		e.setImpressionSource(impressionSource);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPlayStop_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayPause(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPlayPause);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setPlayPercentage(playPercentage);
		e.setImpressionSource(impressionSource);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPlayPause_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayResume(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPlayResume);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setPlayPercentage(playPercentage);
		e.setImpressionSource(impressionSource);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPlayResume_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemRent(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			long rentStartTimestamp, long rentEndTimestamp,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemRent);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setRentStartTimestamp(rentStartTimestamp);
		e.setRentEndTimestamp(rentEndTimestamp);
		e.setImpressionSource(impressionSource);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemRent_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemAddToWatchList(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemAddToWatchList);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setImpressionSource(impressionSource);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemAddToWatchList_WATCHLIST_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPurchase(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			long purchaseStartTimestamp, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemPurchase);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setImpressionSource(impressionSource);
		e.setPurchaseStartTimestamp(purchaseStartTimestamp);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemPurchase_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemImpression(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.setType(EventType.userItemImpression);
		e.setTenantID(tenantId);
		e.setSessionID(sessionId);
		e.setAccountID(accountId);
		e.setTimestamp(timestamp);
		e.setProfileID(profileId);
		e.setItemID(itemId);
		e.setImpressionSource(impressionSource);
		e.setRegionID(regionId);
		e.setLocaleID(localeId);
		e.setDeviceType(deviceType);
		e.setDeviceId(deviceId);
		enqueueAsync(e, RabbitMQConnectorConstants.userItemImpression_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}
}