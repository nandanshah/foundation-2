package com.dla.foundation.analytics.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

public class CassandraContext {
	private Cluster cluster;
	private Session session;

	/**
	 * This method will be used to start mock cassandra.
	 * 
	 * @throws InterruptedException
	 */
	public void connect() throws InterruptedException {
		MockCassandra cassandra = new MockCassandra();
		cassandra.start();
		Thread.sleep(20000);
		cluster = Cluster.builder().addContactPoint("localhost").build();
		session = cluster.connect();
	}

	/**
	 * This method can be used to connect any ip where cassandra is present.
	 * 
	 * @param ip
	 */
	public void connect(String ip) {
		cluster = Cluster.builder().addContactPoint(ip).build();
		session = cluster.connect();
	}

	/**
	 * This method can be used to connect any ip lists where cassandra is
	 * present.
	 * 
	 * @param ip
	 */
	public void connect(String[] ipList) {
		cluster = Cluster.builder().addContactPoints(ipList).build();
		session = cluster.connect();
	}

	public void close() {
		cluster.close();
	}

	/**
	 * This method will execute the provided commands. Befor this it should
	 * connect to cassandra.
	 * 
	 * @param command
	 */
	public void executeCommand(String command) {
		session.execute(command);
	}

	/**
	 * This method will get all the rows of cassandra. Befor this it should
	 * connect to cassandra.
	 * 
	 * @param command
	 */
	public ResultSet getRows(String keyspace, String table) {
		Select query = QueryBuilder.select().all().from(keyspace, table);
		return session.execute(query);
	}
}