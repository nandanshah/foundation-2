package com.dla.foundation.intelligence.eo.consumer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

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
	private List<Thread> consumerThreads;
	private List<Updater> updaters;
	private boolean canStartAllConsumers = true;

	public RMQQueueReceiver(StorageLevel storageLevel) {
		super(storageLevel);
		try {
			qConfig = new QueueListenerConfigHandler();
			logger.info("QueueListenerConfig read");
			consumerThreads = new ArrayList<Thread>();
			updaters = new ArrayList<Updater>();
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
					canStartAllConsumers = false;
					break;
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				canStartAllConsumers = false;
				break;
			}

			if(updater!=null) {
				updaters.add(updater);
				logger.info("Instantiated Updater: " + updater.getClass().getCanonicalName());
				updater.conf = oneQConfig.getUpdaterConf();
				try {
					if(oneQConfig.getType() == QueueListenerConfigHandler.queue_type.sync) {
						logger.info("Starting Synchronous consumer with updater: " + updater.getClass().getCanonicalName());
						consumerThreads.add(new Thread(new SyncQueueConsumer(oneQConfig,updater)));
					} else if(oneQConfig.getType()==QueueListenerConfigHandler.queue_type.async) {
						logger.info("Starting ASynchronous consumer with updater: " + updater.getClass().getCanonicalName());
						consumerThreads.add(new Thread(new AsyncQueueConsumer(oneQConfig,updater)));
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
					canStartAllConsumers = false;
					break;
				}
			}
		}

		//Start all consumers at once.
		if(canStartAllConsumers) {
			for (Thread thread : consumerThreads) {
				thread.start();
			}
		} 
		//If any of the consumer has failed initialization, don't start any other consumer if it's initialized successfully
		//Close updaters and stop this receiver in this case
		else if(!canStartAllConsumers || consumerThreads.size() == 0) {
			for (Updater u : updaters) {
				u.close();
			}
			logger.warn("No consumer to start. Stopping this receiver.");
			this.stop("No consumer to start. Stopping this receiver.");
		}
	}

	@Override
	public void onStop() {
		logger.info("onStop Data Sync Consumer App...");
	}
}