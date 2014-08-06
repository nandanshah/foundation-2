package com.dla.foundation.services.queue.consumer;

import java.io.IOException;
import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;
import com.dla.foundation.services.queue.updater.CassandraUpdater;
import com.dla.foundation.services.queue.updater.ElasticSearchUpdater;
import com.dla.foundation.services.queue.updater.PIOUpdater;
import com.dla.foundation.services.queue.updater.EmailUpdater;
import com.dla.foundation.services.queue.updater.Updater;
import com.dla.foundation.services.queue.util.QueueListenerConfigHandler;
import com.dla.foundation.services.queue.util.QueueListenerConfigHandler.QueueConfig;

/**
 * Spark Streaming container for RabbitMQ consumers (both Sync and ASync).
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class SparkConsumerAMQP implements Runnable {
	
	final Logger logger = Logger.getLogger(this.getClass());
	
	public SparkConsumerAMQP() {

	}

	@Override
	public void run() {
		try {
			JavaStreamingContext ctx = new JavaStreamingContext("local", "ReadQueue", new Duration(1000));
			JavaDStream<AnalyticsCollectionEvent> customReceiverStream = ctx.receiverStream(new QueueReceiverAMQP(StorageLevel.MEMORY_AND_DISK()));
			customReceiverStream.print();	
			logger.info("Starting Spark Streaming Context");
			ctx.start();
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
}

final class QueueReceiverAMQP extends Receiver<AnalyticsCollectionEvent> implements Serializable {

	private static final long serialVersionUID = 8782646200155264653L;
	
	private QueueListenerConfigHandler qConfig;
	private static ElasticSearchUpdater es= null;

	public QueueReceiverAMQP(StorageLevel storageLevel) {
		super(storageLevel);
		try {
			qConfig = new QueueListenerConfigHandler();
			es= new ElasticSearchUpdater();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onStart() {
		Updater updater = null;
		for (QueueConfig oneQConfig : qConfig.getqConfigs()) {
			if(oneQConfig.getUpdater().equalsIgnoreCase("PIOUpdater"))
				updater = new PIOUpdater();
			else if(oneQConfig.getUpdater().equalsIgnoreCase("ElasticSearchUpdater"))
				updater = es;
			else if(oneQConfig.getUpdater().equalsIgnoreCase("CassandraUpdater"))
				updater = new CassandraUpdater();
			else if (oneQConfig.getUpdater().equalsIgnoreCase("EmailUpdater"))
				updater = new EmailUpdater();

			if(oneQConfig.getType()==QueueListenerConfigHandler.queue_type.sync)
				new Thread(new SyncQueueConsumer(oneQConfig,updater)).start();
			else if(oneQConfig.getType()==QueueListenerConfigHandler.queue_type.async)
				new Thread(new AsyncQueueConsumer(oneQConfig,updater)).start();
		}
	}

	@Override
	public void onStop() {

	}
}
