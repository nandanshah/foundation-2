package com.dla.foundation;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import com.dla.foundation.data.FoundationDataContext;
import com.dla.foundation.data.FoundationDataService;
import com.dla.foundation.data.FoundationDataServiceImpl;
import com.dla.foundation.data.entities.recommendations.ScoreUpdateEvent;
import com.dla.foundation.data.persistence.cassandra.CassandraContext;
import com.dla.foundation.services.queue.FoundationQueueMessage;
import com.dla.foundation.services.queue.QueueManager;
import com.dla.foundation.services.queue.connectors.SimpleRabbitMQConnector;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SparkManager implements Runnable, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5367677437015330373L;
	private static final String[] CASSANDRA_IPS = {"localhost"};

	@Override
	public void run() {
		try {
			JavaStreamingContext ctx = new JavaStreamingContext("local", "ReadQueue", new Duration(1000));
			JavaDStream<ScoreUpdateEvent> customReceiverStream = ctx.receiverStream(new QueueReceiverAMQP(StorageLevel.MEMORY_AND_DISK()));
			customReceiverStream.print();	
			ctx.start();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	final class QueueReceiverAMQP extends Receiver<ScoreUpdateEvent> implements Serializable {

		public QueueReceiverAMQP(StorageLevel storageLevel) {
			super(storageLevel);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 7868662019703681720L;

		@Override
		public void onStart() {
			QueueManager manager = new QueueManager(new SimpleRabbitMQConnector("scoreupdate", "10.0.79.178", "test", "test"));
			new Thread(new QueueReader(manager)).start();
		}

		@Override
		public void onStop() {
			
		}
		
	}
	
	final class QueueReader implements Runnable, Serializable {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = -4728781489703110624L;
		private QueueManager manager;
		private FoundationDataService dataService;

		public QueueReader(QueueManager manager) {
			this.manager = manager;
			
			FoundationDataContext data = CassandraContext.create("com.dla.foundation", "n2", CASSANDRA_IPS);
			dataService = new FoundationDataServiceImpl(data);
		}

		@Override
		public void run() {
			FoundationQueueMessage message = manager.dequeue();
			
			ObjectMapper mapper = new ObjectMapper();
			try {
				ScoreUpdateEvent event = mapper.readValue(message.getBody(), ScoreUpdateEvent.class);
				event.isActive = true;
				dataService.removeOldEventsForItemId(event.itemId);
				dataService.addScoreUpdateEvent(event);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
		}


	}
}