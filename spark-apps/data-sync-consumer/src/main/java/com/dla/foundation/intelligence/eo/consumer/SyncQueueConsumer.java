package com.dla.foundation.intelligence.eo.consumer;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dla.foundation.data.entities.event.Event;
import com.dla.foundation.intelligence.eo.filter.FilterException;
import com.dla.foundation.intelligence.eo.updater.Updater;
import com.dla.foundation.intelligence.eo.util.BlockedListenerLogger;
import com.dla.foundation.intelligence.eo.util.QueueListenerConfigHandler.QueueConfig;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Synchronous queue listener. 
 * The messages produced by rabbitmq producer and intended to be handled in synchronous way are consumed by this consumer.
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class SyncQueueConsumer implements Runnable {

	final Logger logger = Logger.getLogger(this.getClass());

	private ConnectionFactory factory;
	private Connection connection;
	private Channel syncChannel;
	private QueueingConsumer consumer;
	private Updater updater;
	private QueueConfig myConfig;

	public SyncQueueConsumer(QueueConfig config, Updater updater) {
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
			syncChannel = connection.createChannel();
			syncChannel.exchangeDeclarePassive(myConfig.getExchangeName());
			syncChannel.queueDeclarePassive(myConfig.getName());
			syncChannel.basicQos(1);
			consumer = new QueueingConsumer(syncChannel);
			syncChannel.basicConsume(myConfig.getName(), false, consumer);
			logger.info("Started Sync queue listener, bound using key: " + myConfig.getBind_key());
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public void run() {
		while (true) {
			QueueingConsumer.Delivery delivery = null;
			BasicProperties props = null;
			BasicProperties replyProps = null;

			try {
				delivery = consumer.nextDelivery();
				props = delivery.getProperties();
				replyProps = new BasicProperties.Builder().correlationId(props.getCorrelationId()).build();

				byte[] obj = delivery.getBody();
				Event fe = Event.fromBytes(obj);
				//Write to an endpoint (such as Cassandra, ElasticSearch, PredictionIO etc.)
				fe = (Event) updater.updateSyncEvent(fe);
				//Push reply message to reply queue defined by producer.
				syncChannel.basicPublish("", props.getReplyTo(), replyProps, fe.toBytes());
			} catch (ShutdownSignalException | ConsumerCancelledException
					| InterruptedException | IOException | FilterException e) {
				logger.error(e.getMessage(), e);
			} finally {
				//Default acknowledgment
				try {
					syncChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
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
			syncChannel.close();
			logger.info("ASync Channel closed");
			connection.close();
			logger.info("Connection closed");
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}
}
