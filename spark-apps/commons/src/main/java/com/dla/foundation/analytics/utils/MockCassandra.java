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

	@Override
	public void run() {
		super.run();
		String current_dir = System.getProperty("user.dir");
		System.out.println(current_dir);
		System.setProperty("cassandra.config", "file:///" + current_dir
				+ "/../commons/src/main/resources/cassandra.yaml");
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