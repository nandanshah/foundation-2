package com.dla.foundation.services.queue.consumer;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;
import com.dla.foundation.services.queue.updater.CassandraUpdater;
import com.dla.foundation.services.queue.updater.ElasticSearchUpdater;
import com.dla.foundation.services.queue.updater.EmailUpdater;
import com.dla.foundation.services.queue.updater.PIOUpdater;
import com.dla.foundation.services.queue.updater.Updater;
import com.dla.foundation.services.queue.util.QueueListenerConfigHandler;
import com.dla.foundation.services.queue.util.QueueListenerConfigHandler.QueueConfig;

public class RMQQueueReceiver extends Receiver<AnalyticsCollectionEvent> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1180932706137696729L;
	
	private QueueListenerConfigHandler qConfig;

	public RMQQueueReceiver(StorageLevel storageLevel) {
		super(storageLevel);
		try {
			qConfig = new QueueListenerConfigHandler();
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
				updater = ElasticSearchUpdater.getInstance();
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