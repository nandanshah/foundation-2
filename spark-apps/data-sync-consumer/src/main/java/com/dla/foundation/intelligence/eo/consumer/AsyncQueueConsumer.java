package com.dla.foundation.intelligence.eo.consumer;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dla.foundation.data.entities.event.Event;
import com.dla.foundation.intelligence.eo.updater.Updater;
import com.dla.foundation.intelligence.eo.util.BlockedListenerLogger;
import com.dla.foundation.intelligence.eo.util.QueueListenerConfigHandler.QueueConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * Asynchronous queue listener. 
 * The messages produced by rabbitmq producer and intended to be handled in asynchronous way are consumed by this consumer.
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class AsyncQueueConsumer implements Runnable {

	final Logger logger = Logger.getLogger(this.getClass());

	private ConnectionFactory factory;
	private Connection connection;
	private Channel asyncChannel;
	private QueueingConsumer consumer;
	private Updater updater;
	private QueueConfig myConfig;

	public AsyncQueueConsumer(QueueConfig config, Updater updater) {
		this.updater = updater;
		this.myConfig = config;

		factory = new ConnectionFactory();

		try {
			factory.setHost(myConfig.getRabbitMQServer());
			factory.setPort(myConfig.getRabbitMQPort());
			factory.setUsername(myConfig.getUsername());
			factory.setPassword(myConfig.getPassword());
			connection = factory.newConnection();
			connection.addBlockedListener(new BlockedListenerLogger());
			asyncChannel = connection.createChannel();
			asyncChannel.exchangeDeclarePassive(myConfig.getExchangeName());
			asyncChannel.queueDeclarePassive(myConfig.getName());
			asyncChannel.basicQos(1);
			consumer = new QueueingConsumer(asyncChannel);
			asyncChannel.basicConsume(myConfig.getName(), false, consumer);
			logger.info("Started ASync queue listener, bound using key: " + myConfig.getBind_key());
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public void run() {
		while (true) {
			QueueingConsumer.Delivery delivery = null;
			try {
				delivery = consumer.nextDelivery();
				byte[] obj = delivery.getBody();
				Event fe = Event.fromBytes(obj);
				//Write to an endpoint (such as Cassandra, ElasticSearch, PredictionIO etc.)
				updater.updateAsyncEvent(fe);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			} finally {
				//Default acknowledgment
				try {
					asyncChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		try {
			asyncChannel.close();
			logger.info("ASync Channel closed");
			connection.close();
			logger.info("Connection closed");
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}
}