package com.dla.foundation.fis.eo.dispatcher;

import java.util.Map;
import java.util.UUID;

import com.dla.foundation.fis.eo.entities.DeviceType;
import com.dla.foundation.fis.eo.entities.EventType;
import com.dla.foundation.fis.eo.entities.FISEvent;
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
public class RabbitMQDispatcher extends AbstractRMQDispatcher<FISEvent> {

	public RabbitMQDispatcher() {
		this.conf = new EOConfig();
	}

	@Override
	public boolean profileAdded(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.profileAdded;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.profileAdded_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean profileDeleted(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.profileDeleted;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.profileDeleted_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean profileUpdatePreferredRegion(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId,
			UUID preferredregionId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.profileUpdatePreferredRegion;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.preferredRegionID = preferredregionId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.profileUpdatePreferredRegion_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean profileUpdatePreferredLocale(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId,
			UUID preferredLocaleId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.profileUpdatePreferredLocale;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.preferredLocaleID = preferredLocaleId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.profileUpdatePreferredLocale_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean profileUpdateNewSocialMediaAccountLinkage(UUID tenantId,
			UUID sessionId, UUID accountId, long timestamp, UUID profileId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId,
			SocialMediaType socialMediaType, String gigyaAuthToken, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.profileUpdateNewSocialMediaAccountLinkage;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.socialMediaType = socialMediaType;
		e.gigyaAuthToken = gigyaAuthToken;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.profileUpdateNewSocialMediaAccountLinkage_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean profileUpdateSocialMediaAccountDelete(UUID tenantId,
			UUID sessionId, UUID accountId, long timestamp, UUID profileId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId,
			SocialMediaType socialMediaType, String gigyaAuthToken, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.profileUpdateSocialMediaAccountDelete;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.socialMediaType = socialMediaType;
		e.gigyaAuthToken = gigyaAuthToken;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.profileUpdateSocialMediaAccountDelete_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean accountAdd(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.accountAdd;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.accountAdd_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean accountDelete(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.accountDelete;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.accountDelete_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean accountInfoUpdate(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.accountInfoUpdate;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.accountInfoUpdate_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userLogin(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.userLogin;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userLogin_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userLogout(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.userLogout;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userLogout_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean addSession(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, Map<String, String> avp,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.addSession;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.avp = avp;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.addSession_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userSearch(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, SearchType searchType,
			String searchString, Map<String, String> filters, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.userSearch;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.searchType = searchType;
		e.searchQuery = searchString;
		e.filters = filters;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userSearch_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userSearchResultClick(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId,
			SearchType searchType, String searchString, int resultPageNumber,
			UUID itemId, int rankOfItemId, UserActions action, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.userSearchResultClick;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.searchType = searchType;
		e.searchQuery = searchString;
		e.resultPageNumber = resultPageNumber;
		e.itemID = itemId;
		e.rankOfItemId = rankOfItemId;
		e.action = action;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userSearchResultClick_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPreview(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.userItemPreview;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPreview_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemMoreInfo(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.userItemMoreInfo;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemMoreInfo_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemShare(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.userItemShare;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemShare_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemRate(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, int rateScore,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.userItemRate;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.rateScore = rateScore;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemRate_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemAddToWatchList(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.userItemAddToWatchList;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemAddToWatchList_WATCHLIST_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemDeleteFromWatchlist(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.userItemDeleteFromWatchlist;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemDeleteFromWatchlist_WATCHLIST_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayStart(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.userItemPlayStart;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPlayStart_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayPercentage(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = EventType.userItemPlayPercentage;
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.playPercentage = playPercentage;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPlayPercentage_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayStop(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemPlayStop);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.playPercentage = (playPercentage);
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPlayStop_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayPause(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemPlayPause);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.playPercentage = (playPercentage);
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPlayPause_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayResume(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemPlayResume);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.playPercentage = (playPercentage);
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPlayResume_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemRent(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			long rentStartTimestamp, long rentEndTimestamp, UUID regionId,
			UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemRent);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.rentStartTimestamp = (rentStartTimestamp);
		e.rentEndTimestamp = (rentEndTimestamp);
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemRent_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPurchase(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			long purchaseStartTimestamp, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemPurchase);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.purchaseStartTimestamp = (purchaseStartTimestamp);
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPurchase_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemImpression(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemImpression);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemImpression_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean accountInfoUpdate(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, Map<String, String> avp,
			UUID regionId, UUID localeId, DeviceType deviceType, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.accountInfoUpdate);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.avp = (avp);
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.accountInfoUpdate_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPreview(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemPreview);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.impressionSource = impressionSource;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPreview_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemMoreInfo(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemMoreInfo);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.impressionSource = impressionSource;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemMoreInfo_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemShare(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemShare);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.impressionSource = impressionSource;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemShare_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemRate(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId, int rateScore,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemRate);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.rateScore = (rateScore);
		e.impressionSource = impressionSource;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemRate_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayStart(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemPlayStart);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.impressionSource = impressionSource;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPlayStart_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayPercentage(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemPlayPercentage);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.playPercentage = (playPercentage);
		e.impressionSource = impressionSource;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPlayPercentage_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayStop(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemPlayStop);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.playPercentage = (playPercentage);
		e.impressionSource = impressionSource;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPlayStop_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayPause(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemPlayPause);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.playPercentage = (playPercentage);
		e.impressionSource = impressionSource;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPlayPause_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPlayResume(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			double playPercentage, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemPlayResume);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.playPercentage = (playPercentage);
		e.impressionSource = impressionSource;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPlayResume_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemRent(UUID tenantId, UUID sessionId, UUID accountId,
			long timestamp, UUID profileId, UUID itemId,
			long rentStartTimestamp, long rentEndTimestamp,
			ImpressionSource impressionSource, DeviceType deviceType,
			UUID regionId, UUID localeId, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemRent);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.rentStartTimestamp = (rentStartTimestamp);
		e.rentEndTimestamp = (rentEndTimestamp);
		e.impressionSource = impressionSource;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemRent_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemAddToWatchList(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemAddToWatchList);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.impressionSource = impressionSource;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemAddToWatchList_WATCHLIST_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemPurchase(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			long purchaseStartTimestamp, ImpressionSource impressionSource,
			DeviceType deviceType, UUID regionId, UUID localeId, UUID deviceId, NetworkType networkType)
					throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemPurchase);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.impressionSource = impressionSource;
		e.purchaseStartTimestamp = (purchaseStartTimestamp);
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemPurchase_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}

	@Override
	public boolean userItemImpression(UUID tenantId, UUID sessionId,
			UUID accountId, long timestamp, UUID profileId, UUID itemId,
			ImpressionSource impressionSource, UUID regionId, UUID localeId,
			DeviceType deviceType, UUID deviceId, NetworkType networkType) throws DispatcherException {
		FISEvent e = new FISEvent();
		e.type = (EventType.userItemImpression);
		e.tenantID = tenantId;
		e.sessionID = sessionId;
		e.accountID = accountId;
		e.timestamp = timestamp;
		e.profileID = profileId;
		e.itemID = itemId;
		e.impressionSource = impressionSource;
		e.regionID = regionId;
		e.localeID = localeId;
		e.deviceType = deviceType;
		e.deviceId = deviceId;
		e.networkType = networkType;
		enqueueAsync(e, RabbitMQDispatcherConstants.userItemImpression_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY);
		return true;
	}
}