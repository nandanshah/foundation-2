package com.dla.foundation.eo.dispatcher;
/*package com.dla.foundation.eo.dispatcher;

import java.io.IOException;
import java.nio.channels.Channel;
import java.sql.Connection;
import java.util.UUID;
import java.util.logging.Logger;

*//**
 * 
 * @author tsudake.psl@dlavideo.com
 * 
 * Basic Rabbit MQ based producer using AMQP.
 * Supports two modes for pushing data to queues, Async and Sync.
 * 
 * @param <E>
 *//*
public class RabbitMQAMQPConnector <E extends AnalyticsCollectionEvent> {

	final Logger logger = Logger.getLogger(this.getClass());

	private String rmq_host = RabbitMQConnectorConstants.RABBITMQ_HOST;
	private int rmq_port = RabbitMQConnectorConstants.RABBITMQ_PORT;
	
	private ConnectionFactory factory;
	private Connection connection;
	private Channel asyncChannel;
	private Channel syncChannel;

	private QueueingConsumer syncConsumer;

	private String syncReplyQueueName;
	
	*//**
	 * Initialize RMQ Producer
	 * @throws IOException
	 *//*
	public RabbitMQAMQPConnector() throws IOException {
		init();
	}

	*//**
	 * Initialize RMQ Producer with non default host and port
	 * 
	 * @param host RabbitMQ Server Host
	 * @param port RabbitMQ Server Port
	 * @throws IOException
	 *//*
	public RabbitMQAMQPConnector(String host, int port) throws IOException {
		this.rmq_host = host;
		this.rmq_port = port;
		init();
	}

	*//**
	 * Push messages asynchronously to RabbitMQ Exchange with routing key route_key.
	 * The call to made with this function returns as soon as message is pushed, it doesn't wait for reply from consumers.
	 * 
	 * @param message Message to be sent
	 * @param route_key Routing key of the message. Consumers listening on queues registered with matching binding key will receive this message.
	 *//*
	public void enqueueAsync(E message, String route_key) {
		if(null!=message) {
			byte[] obj = message.getBytes();
			try {
				asyncChannel.basicPublish(RabbitMQConnectorConstants.EXCHANGE_NAME, route_key, MessageProperties.PERSISTENT_TEXT_PLAIN, obj);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		else
			logger.error("Message is null");
	}

	*//**
	 * Push messages synchronously to RabbitMQ Exchange with routing key route_key.
	 * This function waits till reply message is received from consumers.
	 * 
	 * @param message Message to be sent
	 * @param route_key Routing key of the message. Consumers listening on queues registered with matching binding key will receive this message.
	 *//*
	public AnalyticsCollectionEvent enqueueSync(E message, String route_key) {
		AnalyticsCollectionEvent ret = null;
		if(null!=message) {
			byte[] obj = message.getBytes();
			String corrId = UUID.randomUUID().toString();
			BasicProperties props = new BasicProperties.Builder().correlationId(corrId).replyTo(syncReplyQueueName).build();
			try {
				syncChannel.basicPublish(RabbitMQConnectorConstants.EXCHANGE_NAME, route_key, props, obj);
				while (true) {
					QueueingConsumer.Delivery delivery = syncConsumer.nextDelivery();
					if (delivery.getProperties().getCorrelationId().equals(corrId)) {
						ret =  AnalyticsCollectionEvent.fromBytes(delivery.getBody());
						break;
					}
				}
			} catch (IOException | ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		} else
			logger.error("Message is null");
		return ret;
	}

	*//**
	 * Dequeue message from message queue.
	 * Currently not implemented.
	 * 
	 * @return
	 *//*
	public E dequeue() {
		throw new NotImplementedException();
	}

	public void init() throws IOException {
		factory = new ConnectionFactory();
		factory.setHost(rmq_host);
		factory.setPort(rmq_port);
		connection = factory.newConnection();
		asyncChannel = connection.createChannel();
		asyncChannel.exchangeDeclare(RabbitMQConnectorConstants.EXCHANGE_NAME, RabbitMQConnectorConstants.EXCHANGE_TYPE);

		syncChannel = connection.createChannel();
		syncChannel.exchangeDeclare(RabbitMQConnectorConstants.EXCHANGE_NAME, RabbitMQConnectorConstants.EXCHANGE_TYPE);

		syncReplyQueueName = syncChannel.queueDeclare().getQueue(); 
		syncConsumer = new QueueingConsumer(syncChannel);
		syncChannel.basicConsume(syncReplyQueueName, true, syncConsumer);
		logger.info("RabbitMQ Connector initialized with Host: " + rmq_host + ", port: " + rmq_port + ", exchange: " + RabbitMQConnectorConstants.EXCHANGE_NAME);
	}

	*//**
	 * Closes connections to RabbitMQ Server and Exchange.
	 *//*
	public void close() {
		try {
			asyncChannel.close();
			syncChannel.close();
			connection.close();
			logger.info("RabbitMQ Connector closed");
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	*//**
	 * Resets this connector.
	 * Releases resources held and reinitializes.
	 * @throws IOException
	 *//*
	public void reset() throws IOException {
		close();
		init();
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		close();
	}
}*/