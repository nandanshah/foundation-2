package com.dla.foundation.services.queue.consumer;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dla.foundation.data.entities.analytics.UserEvent;
import com.dla.foundation.fis.eo.entities.FISUserEvent;
import com.dla.foundation.services.queue.updater.Updater;
import com.dla.foundation.services.queue.util.QueueListenerConfigHandler.QueueConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

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
			connection = factory.newConnection();
			asyncChannel = connection.createChannel();
			asyncChannel.exchangeDeclare(myConfig.getExchangeName(), myConfig.getExchangeType());
			String queueName = asyncChannel.queueDeclare().getQueue();
			asyncChannel.queueBind(queueName, myConfig.getExchangeName(), myConfig.getBind_key());
			asyncChannel.basicQos(1);
			consumer = new QueueingConsumer(asyncChannel);
			asyncChannel.basicConsume(queueName, false, consumer);
			logger.info("Started ASync queue listener, bound using key: " + myConfig.getBind_key());
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public void run() {
		while (true) {
			QueueingConsumer.Delivery delivery;
			try {
				delivery = consumer.nextDelivery();
				byte[] obj = delivery.getBody();
				FISUserEvent fe = FISUserEvent.fromBytes(obj);
				UserEvent ue = UserEvent.copy(fe);
				//Write to an endpoint (such as Cassandra, ElasticSearch, PredictionIO etc.)
				updater.updateAsyncEvent(ue);
				//Default acknowledgment
				asyncChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			} catch (ShutdownSignalException | ConsumerCancelledException
					| InterruptedException | IOException e) {
				logger.error(e.getMessage(), e);
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