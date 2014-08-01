package com.dla.foundation.eo.dispatcher;

/**
 * 
 * @author tsudake.psl@dlavideo.com
 * 
 * Holds default values of configuration properties used by RabbitMQ Producer
 */
public class RabbitMQConnectorConstants {
	
	//TODO Make these configurations as dynamic as possible by externalizing them to a property file 
	public static final String RABBITMQ_HOST = "10.0.79.178";
	public static final int RABBITMQ_PORT = 5672;
	
	public static final String EXCHANGE_NAME = "FOUNDATION_EXCH";
	public static final String EXCHANGE_TYPE = "topic";
	
	public static final String ITEM_ADDED_WATCHLIST_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "itemadded.watchlist.cassandra.async.event.message";
	public static final String ITEM_ADDED_WATCHLIST_SERVICE_CASSANDRA_SYNC_ROUTE_KEY = "itemadded.watchlist.cassandra.sync.event.message";
	public static final String ITEM_ADDED_WATCHLIST_SERVICE_PIO_ASYNC_ROUTE_KEY = "itemadded.watchlist.pio.async.event.message";
	public static final String ITEM_ADDED_WATCHLIST_SERVICE_PIO_SYNC_ROUTE_KEY = "itemadded.watchlist.pio.sync.event.message";
	public static final String ITEM_ADDED_WATCHLIST_SERVICE_ES_ASYNC_ROUTE_KEY = "itemadded.watchlist.es.async.event.message";
	public static final String ITEM_ADDED_WATCHLIST_SERVICE_ES_SYNC_ROUTE_KEY = "itemadded.watchlist.es.sync.event.message";

	public static final String ITEM_REM_WATCHLIST_SERVICE_CASSANDRA_ASYNC_ROUTE_KEY = "itemremoved.watchlist.cassandra.async.event.message";
	public static final String ITEM_REM_WATCHLIST_SERVICE_CASSANDRA_SYNC_ROUTE_KEY = "itemremoved.watchlist.cassandra.sync.event.message";
	public static final String ITEM_REM_WATCHLIST_SERVICE_PIO_ASYNC_ROUTE_KEY = "itemremoved.watchlist.pio.async.event.message";
	public static final String ITEM_REM_WATCHLIST_SERVICE_PIO_SYNC_ROUTE_KEY = "itemremoved.watchlist.pio.sync.event.message";
	public static final String ITEM_REM_WATCHLIST_SERVICE_ES_ASYNC_ROUTE_KEY = "itemremoved.watchlist.es.async.event.message";
	public static final String ITEM_REM_WATCHLIST_SERVICE_ES_SYNC_ROUTE_KEY = "itemremoved.watchlist.es.sync.event.message";
	
	public static final String ITEM_SCORE_UPDATE_EVENT_ASYNC = "scoreupdate.async.event.message";
	public static final String RABBITMQ_USERNAME = "test";
	public static final String RABBITMQ_PASSWORD = "test";
}
