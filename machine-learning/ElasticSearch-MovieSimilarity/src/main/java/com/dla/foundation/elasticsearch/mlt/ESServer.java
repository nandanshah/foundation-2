package com.dla.foundation.elasticsearch.mlt;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ESServer {

	private String serverIP;
	private int serverPort ;
	private String clusterName;
	Client client;

	public ESServer() {
	}

	public ESServer(String serverIP, int serverPort, String clusterName) {
		this.serverIP = serverIP;
		this.serverPort = serverPort;
		this.clusterName = clusterName;
	}

	public Client getESClient() {
		// ES Node setup
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", clusterName).build();
		this.client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(serverIP,
						serverPort));
		return this.client;
	}

	void closeConnection() {
		this.client.close();
	}

}
