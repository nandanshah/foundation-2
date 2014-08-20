package com.dla.foundation.fis.eo.dispatcher;

import com.dla.foundation.fis.eo.dispatcher.RabbitMQDispatcherConstants;

public class EOConfig {

	public String rmqHost = RabbitMQDispatcherConstants.RABBITMQ_HOST;
	public int rmqPort = RabbitMQDispatcherConstants.RABBITMQ_PORT;
	public String exchangeName = RabbitMQDispatcherConstants.EXCHANGE_NAME;
	public String exchangeType = RabbitMQDispatcherConstants.EXCHANGE_TYPE;
	public String username = RabbitMQDispatcherConstants.DEFAULT_USER;
	public String password = RabbitMQDispatcherConstants.DEFAULT_PWD;
	
	public EOConfig() {
		
	}
	
	public EOConfig(String rmqHost, int rmqPort) {
		super();
		this.rmqHost = rmqHost;
		this.rmqPort = rmqPort;
	}

	public EOConfig(String rmqHost, int rmqPort, String eXCHANGE_NAME,
			String eXCHANGE_TYPE, String username, String password) {
		super();
		this.rmqHost = rmqHost;
		this.rmqPort = rmqPort;
		this.exchangeName = eXCHANGE_NAME;
		this.exchangeType = eXCHANGE_TYPE;
		this.username = username;
		this.password = password;
	}
}