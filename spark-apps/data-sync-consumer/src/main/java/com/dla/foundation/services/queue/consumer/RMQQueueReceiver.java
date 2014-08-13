package com.dla.foundation.services.queue.consumer;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;
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
			Class<? extends Updater> updaterClass = null;
			try {
				updaterClass = (Class<Updater>) Class.forName(oneQConfig.getUpdater());
				updater = updaterClass.newInstance();
			} catch (IllegalAccessException e) {
				try {
					Method instanceMthd = updaterClass.getMethod("getInstance", null);
					updater = (Updater) instanceMthd.invoke(null, null);

					if(oneQConfig.getType()==QueueListenerConfigHandler.queue_type.sync)
						new Thread(new SyncQueueConsumer(oneQConfig,updater)).start();
					else if(oneQConfig.getType()==QueueListenerConfigHandler.queue_type.async)
						new Thread(new AsyncQueueConsumer(oneQConfig,updater)).start();
				} catch (NoSuchMethodException | SecurityException | IllegalAccessException 
						| IllegalArgumentException | InvocationTargetException e1) {
					e1.printStackTrace();
				}
			} catch (ClassNotFoundException | InstantiationException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void onStop() {

	}
}