package com.dla.foundation.eo.dispatcher;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dla.foundation.eo.entities.EOConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


/**
 * This mehtod will provide the implementation of initializing rabbitMQ and close connection for it.
 * @author shishir_shivhare
 *
 */
public abstract class AbstractRMQDispatcher implements EODispatcher{

	final Logger logger = Logger.getLogger(this.getClass());
	public ConnectionFactory factory;
	public Connection connection;
	public Channel asyncChannel;
		
	@Override
	public void init(EOConfig eoConfig) throws IOException {
		factory = new ConnectionFactory();
		factory.setHost(eoConfig.rmqHost);
		factory.setPort(eoConfig.rmqPort);
		connection = factory.newConnection();
		asyncChannel = connection.createChannel();
		asyncChannel.exchangeDeclare(RabbitMQConnectorConstants.EXCHANGE_NAME, RabbitMQConnectorConstants.EXCHANGE_TYPE);
		logger.info("RabbitMQ Connector initialized with Host: " + eoConfig.rmqHost + ", port: " + eoConfig.rmqHost + ", exchange: " + RabbitMQConnectorConstants.EXCHANGE_NAME);

		
	}
	
	@Override
	public void close() throws IOException {
			asyncChannel.close();
			connection.close();
			logger.info("RabbitMQ Connector closed");
	}
}
