package com.dla.foundation.intelligence.eo.consumer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.log4j.Logger;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import com.dla.foundation.data.persistence.SimpleFoundationEntity;
import com.dla.foundation.intelligence.eo.updater.Updater;
import com.dla.foundation.intelligence.eo.util.QueueListenerConfigHandler;
import com.dla.foundation.intelligence.eo.util.QueueListenerConfigHandler.QueueConfig;

public class RMQQueueReceiver extends Receiver<SimpleFoundationEntity> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -44917923667742208L;

	private static final Logger logger = Logger.getLogger(RMQQueueReceiver.class);

	private QueueListenerConfigHandler qConfig;

	public RMQQueueReceiver(StorageLevel storageLevel) {
		super(storageLevel);
		try {
			qConfig = new QueueListenerConfigHandler();
			logger.info("QueueListenerConfig read");
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public void onStart() {
		Updater updater = null;
		for (QueueConfig oneQConfig : qConfig.getqConfigs()) {
			updater = null;
			Class<Updater> updaterClass = null;
			try {
				updaterClass = (Class<Updater>) Class.forName(oneQConfig.getUpdater());
				updater = updaterClass.newInstance();
			} catch (IllegalAccessException e) {
				logger.error(e.getMessage(), e);
				try {
					Method instanceMthd = updaterClass.getMethod("getInstance", null);
					updater = (Updater) instanceMthd.invoke(null, null);
				} catch (NoSuchMethodException | SecurityException | IllegalAccessException 
						| IllegalArgumentException | InvocationTargetException e1) {
					logger.error(e1.getMessage(), e1);
				}
			} catch (ClassNotFoundException | InstantiationException e) {
				logger.error(e.getMessage(), e);
			}

			if(updater!=null) {
				logger.info("Instantiated Updater: " + updater.getClass().getCanonicalName());
				updater.conf = oneQConfig.getUpdaterConf();
				if(oneQConfig.getType() == QueueListenerConfigHandler.queue_type.sync) {
					logger.info("Starting Synchronous consumer with updater: " + updater.getClass().getCanonicalName());
					new Thread(new SyncQueueConsumer(oneQConfig,updater)).start();
				} else if(oneQConfig.getType()==QueueListenerConfigHandler.queue_type.async) {
					logger.info("Starting ASynchronous consumer with updater: " + updater.getClass().getCanonicalName());
					new Thread(new AsyncQueueConsumer(oneQConfig,updater)).start();
				}
			}
		}
	}

	@Override
	public void onStop() {
		logger.info("onStop Data Sync Consumer App...");
	}
}