package com.dla.foundation.fis.eo.entities;

import com.dla.foundation.fis.eo.dispatcher.RabbitMQConnectorConstants;

public class EOConfig {

	public String rmqHost = RabbitMQConnectorConstants.RABBITMQ_HOST;
	public int rmqPort = RabbitMQConnectorConstants.RABBITMQ_PORT;
	public String EXCHANGE_NAME = RabbitMQConnectorConstants.EXCHANGE_NAME;
	public String EXCHANGE_TYPE = RabbitMQConnectorConstants.EXCHANGE_TYPE;
	
	public EOConfig() {
		
	}
	
	public EOConfig(String rmqHost, int rmqPort) {
		super();
		this.rmqHost = rmqHost;
		this.rmqPort = rmqPort;
	}

	public EOConfig(String rmqHost, int rmqPort, String eXCHANGE_NAME,
			String eXCHANGE_TYPE) {
		super();
		this.rmqHost = rmqHost;
		this.rmqPort = rmqPort;
		EXCHANGE_NAME = eXCHANGE_NAME;
		EXCHANGE_TYPE = eXCHANGE_TYPE;
	}
}