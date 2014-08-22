package com.dla.foundation.analytics.utils;

import java.io.IOException;

import org.apache.cassandra.service.EmbeddedCassandraService;

/**
 * This class will provide the functionality to start mock cassandra.
 * 
 * @author shishir_shivhare
 * 
 */
public class MockCassandra extends Thread {
	private static EmbeddedCassandraService cassandra;
	private String config_path;

	/**
	 * Initilizes MockCassandra with valid config_path.
	 * @param config_path
	 */
	public MockCassandra(String config_path) {
		this.config_path = config_path;
	}

	@Override
	public void run() {
		super.run();
		System.setProperty("cassandra.config", config_path);
		try {
			if (cassandra == null) {
				cassandra = new EmbeddedCassandraService();
				cassandra.start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}