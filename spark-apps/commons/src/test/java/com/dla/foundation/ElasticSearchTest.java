package com.dla.foundation;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;
import com.dla.foundation.data.persistence.elasticsearch.ElasticSearchRepo;
import com.dla.foundation.data.persistence.elasticsearch.UserRecommendation;
import com.dla.foundation.services.contentdiscovery.entities.MediaItem;
import com.dla.foundation.services.queue.connectors.RabbitMQAMQPConnector;
import com.dla.foundation.services.queue.connectors.RabbitMQConnectorConstants;

/*
 *  This junit will not be executed for below reasons
 *  	The EO code has dependency on platform project and the platform project cannot be built till mavenisation issue is fixed.
 *  	Also for building this junit the spark consumer in 'foundation-intelligence' should be up and running
 */
@Ignore
public class ElasticSearchTest {

	private static String esHost= "http://localhost:9200/";
	private static ElasticSearchRepo repository= null;
	private static RabbitMQAMQPConnector<AnalyticsCollectionEvent> rc;


	@BeforeClass
	public static void beforeClass() throws IOException{
		repository= new ElasticSearchRepo(esHost);
		rc = new RabbitMQAMQPConnector<AnalyticsCollectionEvent>();
	}

	@Test
	public void testAddSyncEvent() throws Exception{
		UUID id4 = UUID.randomUUID();
		AnalyticsCollectionEvent i4 = new AnalyticsCollectionEvent();
		i4.id = id4;
		i4.customEventLabel="MovieAdded";
		i4.customEventValue="9107";
		rc.enqueueSync(i4, RabbitMQConnectorConstants.ITEM_ADDED_WATCHLIST_SERVICE_ES_SYNC_ROUTE_KEY);

		MediaItem md= repository.getMediaItem("catalog", "movie", i4.customEventValue);
		assertEquals(md.id, i4.customEventValue);
	}

	@Test
	public void testUpdateSyncEvent() throws Exception{
		UUID id1 = UUID.randomUUID();
		AnalyticsCollectionEvent i1 = new AnalyticsCollectionEvent();
		i1.id = id1;
		i1.customEventLabel="MovieUpdated";
		i1.customEventValue="9107";
		i1.customMetric= "2 days in valley";
		rc.enqueueSync(i1, RabbitMQConnectorConstants.ITEM_ADDED_WATCHLIST_SERVICE_ES_SYNC_ROUTE_KEY);

		MediaItem md= repository.getMediaItem("catalog", "movie", i1.customEventValue);
		assertEquals(md.title, i1.customMetric);
	}

	@Test
	public void testBulkAsyncEvents() throws Exception{
		UUID id1 = UUID.randomUUID();
		UUID id2 = UUID.randomUUID();
		UUID id3 = UUID.randomUUID();
		UUID id4 = UUID.randomUUID();
		UUID id5 = UUID.randomUUID();
		UUID id6 = UUID.randomUUID();
		UUID id7 = UUID.randomUUID();
		UUID id8 = UUID.randomUUID();
		UUID id9 = UUID.randomUUID();

		AnalyticsCollectionEvent i1 = new AnalyticsCollectionEvent();
		i1.id = id1;
		i1.customEventLabel="MovieAdded";
		i1.customEventValue="1020";

		AnalyticsCollectionEvent i2 = new AnalyticsCollectionEvent();
		i2.id = id2;
		i2.customEventLabel="MovieUpdated";
		i2.customEventValue="1018";
		i2.customMetric="2 Days in the Valley";

		AnalyticsCollectionEvent i4 = new AnalyticsCollectionEvent();
		i4.id = id4;
		i4.customEventLabel="UserRecoAdded";
		i4.customEventValue="10009";
		i4.linkId="1000";

		AnalyticsCollectionEvent i5 = new AnalyticsCollectionEvent();
		i5.id = id5;
		i5.customEventLabel="UserRecoUpdated";
		i5.customEventValue="10009";
		i5.customMetric="5";
		i5.linkId="1000";

		AnalyticsCollectionEvent i3 = new AnalyticsCollectionEvent();
		i3.id = id3;
		i3.customEventLabel="UserRecoDeleted";
		i3.customEventValue="10006";
		i3.linkId="1000";

		AnalyticsCollectionEvent i6 = new AnalyticsCollectionEvent();
		i6.id = id6;
		i6.customEventLabel="MovieDeleted";
		i6.customEventValue="1014";

		AnalyticsCollectionEvent i7 = new AnalyticsCollectionEvent();
		i7.id = id7;
		i7.customEventLabel="MovieAdded";
		i7.customEventValue="1012";

		AnalyticsCollectionEvent i8 = new AnalyticsCollectionEvent();
		i8.id = id8;
		i8.customEventLabel="MovieAdded";
		i8.customEventValue="1015";

		AnalyticsCollectionEvent i9 = new AnalyticsCollectionEvent();
		i9.id = id9;
		i9.customEventLabel="UserRecoAdded";
		i9.customEventValue="10006";
		i9.linkId="1000";

		AnalyticsCollectionEvent i10 = new AnalyticsCollectionEvent();


		rc.enqueueAsync(i1, RabbitMQConnectorConstants.ITEM_ADDED_WATCHLIST_SERVICE_ES_ASYNC_ROUTE_KEY);
		rc.enqueueAsync(i2, RabbitMQConnectorConstants.ITEM_ADDED_WATCHLIST_SERVICE_ES_ASYNC_ROUTE_KEY);
		rc.enqueueAsync(i4, RabbitMQConnectorConstants.ITEM_ADDED_WATCHLIST_SERVICE_ES_ASYNC_ROUTE_KEY);
		rc.enqueueAsync(i5, RabbitMQConnectorConstants.ITEM_ADDED_WATCHLIST_SERVICE_ES_ASYNC_ROUTE_KEY);
		rc.enqueueAsync(i6, RabbitMQConnectorConstants.ITEM_ADDED_WATCHLIST_SERVICE_ES_ASYNC_ROUTE_KEY);
		rc.enqueueAsync(i3, RabbitMQConnectorConstants.ITEM_ADDED_WATCHLIST_SERVICE_ES_ASYNC_ROUTE_KEY);
		rc.enqueueAsync(i7, RabbitMQConnectorConstants.ITEM_ADDED_WATCHLIST_SERVICE_ES_ASYNC_ROUTE_KEY);
		rc.enqueueAsync(i8, RabbitMQConnectorConstants.ITEM_ADDED_WATCHLIST_SERVICE_ES_ASYNC_ROUTE_KEY);
		rc.enqueueAsync(i9, RabbitMQConnectorConstants.ITEM_ADDED_WATCHLIST_SERVICE_ES_ASYNC_ROUTE_KEY);
		rc.enqueueAsync(i10, RabbitMQConnectorConstants.ITEM_ADDED_WATCHLIST_SERVICE_ES_ASYNC_ROUTE_KEY);
		Thread.sleep(5000);

		MediaItem item= repository.getMediaItem("catalog", "movie", i1.customEventValue);
		assertEquals(item.id, i1.customEventValue);

		MediaItem item1= repository.getMediaItem("catalog", "movie", i2.customEventValue);
		assertEquals(item1.title, i2.customMetric);

		MediaItem item2= repository.getMediaItem("catalog", "movie", i8.customEventValue);
		assertEquals(item2.id, i8.customEventValue);

		MediaItem item3= repository.getMediaItem("catalog", "movie", i6.customEventValue);
		assertNull(item3);

		UserRecommendation userreco= repository.getUserRecoItem("catalog", "user_reco",i5.customEventValue , i5.linkId);
		assertEquals(userreco.userid, i5.customMetric);
	}
}