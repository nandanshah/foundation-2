package com.dla.foundation.services.analytics;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.UUID;

import org.junit.Test;

import com.dla.foundation.DependencyLocator;
import com.dla.foundation.data.entities.recommendations.ScoreUpdateEvent.ScoreBucket;
import com.dla.foundation.rest.FoundationServiceResponse;
import com.dla.foundation.services.analytics.ScoreService;
import com.dla.foundation.services.analytics.ScoreService.Post;
import com.dla.foundation.services.analytics.ScoreService.UpdateScorePost;
import com.dla.foundation.services.queue.QueueManager;
import com.dla.foundation.services.queue.connectors.SimpleRabbitMQConnector;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EnqueueTest {
	
	@Test
	public void shouldEnqueueMessage() {
		DependencyLocator dependencyLocator = new DependencyLocator();
		
		QueueManager manager = new QueueManager(new SimpleRabbitMQConnector("scoreupdate", "10.0.79.178", "test", "test"));
		dependencyLocator.register(QueueManager.class, manager);
		
		ObjectMapper mapper = new ObjectMapper();
		dependencyLocator.register(ObjectMapper.class, mapper);
		
		ScoreService service = new ScoreService();
		service.dependencyLocator = dependencyLocator;

		ScoreService.Post post = service.new Post();
		

		ScoreService.UpdateScorePost updateScorePost = service.new UpdateScorePost();
		updateScorePost.regionId = UUID.randomUUID();
		updateScorePost.dateTime = new Date();
		updateScorePost.eventType = 3;
		updateScorePost.featuredBoost = ScoreBucket.THREE;
		updateScorePost.featuredEndDate = new Date();
		updateScorePost.featuredStartDate = new Date();
		updateScorePost.freshnessBucket = ScoreBucket.THREE;
		updateScorePost.itemId = UUID.fromString("003ce9dd-3af1-4b50-98b8-680047e79507");
		updateScorePost.popularityBucket = ScoreBucket.FIVE;
		updateScorePost.regionId = UUID.randomUUID();
		updateScorePost.tenantId = UUID.randomUUID();

		post.body = updateScorePost;
		
		FoundationServiceResponse<ScoreService.ScoreResult> response = post.executeRequest();
		System.out.println("response status: " + response.status);
		System.out.println("the response: " + response.result);
		
		assertTrue(response.success);
	}

}
