package com.dla.foundation.fis.eo.dispatcher;

import java.io.IOException;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.dla.foundation.fis.eo.entities.AnalyticsEvent;
import com.dla.foundation.fis.eo.entities.EOConfig;
import com.dla.foundation.fis.eo.exception.DispatcherException;
import com.dla.foundation.fis.eo.exception.NullMessageDispatcherException;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * This mehtod will provide the implementation of initializing rabbitMQ and close connection for it.
 * 
 * @author shishir_shivhare
 *
 */
public abstract class AbstractRMQDispatcher<E extends AnalyticsEvent> implements EODispatcher {

	final Logger logger = Logger.getLogger(this.getClass());

	protected EOConfig conf;

	private ConnectionFactory factory;
	private Connection connection;
	private Channel asyncChannel;
	private Channel syncChannel;

	private QueueingConsumer syncConsumer;

	private String syncReplyQueueName;

	@Override
	public void init(EOConfig eoConfig) throws IOException {
		this.conf = eoConfig;
		init();
	}

	private void init() throws IOException {
		factory = new ConnectionFactory();
		factory.setHost(conf.rmqHost);
		factory.setPort(conf.rmqPort);
		connection = factory.newConnection();
		asyncChannel = connection.createChannel();
		asyncChannel.exchangeDeclare(RabbitMQConnectorConstants.EXCHANGE_NAME, RabbitMQConnectorConstants.EXCHANGE_TYPE);

		syncChannel = connection.createChannel();
		syncChannel.exchangeDeclare(RabbitMQConnectorConstants.EXCHANGE_NAME, RabbitMQConnectorConstants.EXCHANGE_TYPE);

		syncReplyQueueName = syncChannel.queueDeclare().getQueue();
		syncConsumer = new QueueingConsumer(syncChannel);
		syncChannel.basicConsume(syncReplyQueueName, true, syncConsumer);
		logger.info("RabbitMQ Connector initialized with Host: " + conf.rmqHost + ", port: " + conf.rmqPort + ", exchange: " + RabbitMQConnectorConstants.EXCHANGE_NAME);
	}

	/**
	 * Push messages asynchronously to RabbitMQ Exchange with routing key route_key.
	 * The call to made with this function returns as soon as message is pushed, it doesn't wait for reply from consumers.
	 * 
	 * @param message Message to be sent
	 * @param route_key Routing key of the message. Consumers listening on queues registered with matching binding key will receive this message.
	 * @throws DispatcherException 
	 */
	public void enqueueAsync(E message, String route_key) throws DispatcherException {
		if(null!=message) {
			byte[] obj = message.getBytes();
			try {
				asyncChannel.basicPublish(RabbitMQConnectorConstants.EXCHANGE_NAME, route_key, MessageProperties.PERSISTENT_TEXT_PLAIN, obj);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		else
			throw new NullMessageDispatcherException();
	}

	/**
	 * Push messages synchronously to RabbitMQ Exchange with routing key route_key.
	 * This function waits till reply message is received from consumers.
	 * 
	 * @param message Message to be sent
	 * @param route_key Routing key of the message. Consumers listening on queues registered with matching binding key will receive this message.
	 */
	public E enqueueSync(E message, String route_key) throws DispatcherException {
		E ret = null;
		if(null!=message) {
			byte[] obj = message.getBytes();
			String corrId = UUID.randomUUID().toString();
			BasicProperties props = new BasicProperties.Builder().correlationId(corrId).replyTo(syncReplyQueueName).build();
			try {
				syncChannel.basicPublish(RabbitMQConnectorConstants.EXCHANGE_NAME, route_key, props, obj);
				while (true) {
					QueueingConsumer.Delivery delivery = syncConsumer.nextDelivery();
					if (delivery.getProperties().getCorrelationId().equals(corrId)) {
						ret =  (E) AnalyticsEvent.fromBytes(delivery.getBody());
						break;
					}
				}
			} catch (IOException | ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		} else
			throw new NullMessageDispatcherException();
		return ret;
	}

	/**
	 * Dequeue message from message queue.
	 * Currently not implemented.
	 * 
	 * @return
	 * @throws Exception 
	 */
	public E dequeue() throws Exception {
		throw new Exception("Not implemented exception");
	}

	/**
	 * Resets this connector.
	 * Releases resources held and reinitializes.
	 * @throws IOException
	 */
	public void reset() throws IOException {
		close();
		init();
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		close();
	}

	/**
	 * Closes connections to RabbitMQ Server and Exchange.
	 * 
	 */
	@Override
	public void close() throws IOException {
		try {
			asyncChannel.close();
			syncChannel.close();
			connection.close();
			logger.info("RabbitMQ Connector closed");
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}
}
