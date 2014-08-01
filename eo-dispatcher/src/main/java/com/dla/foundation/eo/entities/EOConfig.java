package com.dla.foundation.eo.entities;

public class EOConfig {

	public String rmqHost;
	public int rmqPort;
	public EOConfig(String rmqHost, int rmqPort) {
		super();
		this.rmqHost = rmqHost;
		this.rmqPort = rmqPort;
	}
	
}
