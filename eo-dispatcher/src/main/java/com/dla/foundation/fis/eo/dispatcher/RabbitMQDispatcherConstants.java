package com.dla.foundation.fis.eo.dispatcher;

/**
 * 
 * @author tsudake.psl@dlavideo.com
 * 
 * Holds default values of configuration properties used by RabbitMQ Producer
 */
public class RabbitMQDispatcherConstants {
	
	/* RabbitMQ Server properties */
	public static final String RABBITMQ_HOST = "localhost";
	public static final int RABBITMQ_PORT = 5672;
	
	/* RabbitMQ Exchange properties */
	public static final String EXCHANGE_NAME = "FOUNDATION_EXCH";
	public static final String EXCHANGE_TYPE = "topic";
	
	public static final String DEFAULT_USER = "guest";
	public static final String DEFAULT_PWD = "guest";
	
	/* Message routing keys for events/messages propagating to Cassandra */
	public static final String profileAdded_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "profileAdded.profile.cassandra.async.event.message";
	public static final String profileDeleted_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "profileDeleted.profile.cassandra.async.event.message";
	public static final String profileUpdatePreferredRegion_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "profileUpdatePreferredRegion.profile.cassandra.async.event.message";
	public static final String profileUpdatePreferredLocale_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "profileUpdatePreferredLocale.profile.cassandra.async.event.message";
	public static final String profileUpdateNewSocialMediaAccountLinkage_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "profileUpdateNewSocialMediaAccountLinkage.profile.cassandra.async.event.message";
	public static final String profileUpdateSocialMediaAccountDelete_PROFILE_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "profileUpdateSocialMediaAccountDelete.profile.cassandra.async.event.message";
	
	public static final String accountAdd_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "accountAdd.account.cassandra.async.event.message";
	public static final String accountDelete_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "accountDelete.account.cassandra.async.event.message";
	public static final String accountInfoUpdate_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "accountInfoUpdate.account.cassandra.async.event.message";
	public static final String userLogin_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userLogin.account.cassandra.async.event.message";
	public static final String userLogout_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userLogout.account.cassandra.async.event.message";
	public static final String addSession_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "addSession.account.cassandra.async.event.message";
	public static final String userItemRent_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemRent.account.cassandra.async.event.message";
	public static final String userItemPurchase_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemPurchase.account.cassandra.async.event.message";
	public static final String userItemImpression_ACCOUNT_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemImpression.account.cassandra.async.event.message";
	
	public static final String userSearch_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userSearch.search.cassandra.async.event.message";
	public static final String userSearchResultClick_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userSearchResultClick.search.cassandra.async.event.message";
	public static final String userItemPreview_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemPreview.search.cassandra.async.event.message";
	public static final String userItemMoreInfo_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemMoreInfo.search.cassandra.async.event.message";
	public static final String userItemShare_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemShare.search.cassandra.async.event.message";
	public static final String userItemRate_SEARCH_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemRate.search.cassandra.async.event.message";
	
	public static final String userItemAddToWatchList_WATCHLIST_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemAddToWatchList.watchlist.cassandra.async.event.message";
	public static final String userItemDeleteFromWatchlist_WATCHLIST_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemDeleteFromWatchlist.watchlist.cassandra.async.event.message";
	
	public static final String userItemPlayStart_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemPlayStart.playback.cassandra.async.event.message";
	public static final String userItemPlayPercentage_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemPlayPercentage.playback.cassandra.async.event.message";
	public static final String userItemPlayStop_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemPlayStop.playback.cassandra.async.event.message";
	public static final String userItemPlayPause_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemPlayPause.playback.cassandra.async.event.message";
	public static final String userItemPlayResume_PLAYBACK_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "userItemPlayResume.playback.cassandra.async.event.message";
}
