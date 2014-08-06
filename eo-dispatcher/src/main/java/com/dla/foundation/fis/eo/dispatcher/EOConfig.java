package com.dla.foundation.fis.eo.dispatcher;

import com.dla.foundation.fis.eo.dispatcher.RabbitMQDispatcherConstants;

public class EOConfig {

	public String rmqHost = RabbitMQDispatcherConstants.RABBITMQ_HOST;
	public int rmqPort = RabbitMQDispatcherConstants.RABBITMQ_PORT;
	public String EXCHANGE_NAME = RabbitMQDispatcherConstants.EXCHANGE_NAME;
	public String EXCHANGE_TYPE = RabbitMQDispatcherConstants.EXCHANGE_TYPE;
	
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