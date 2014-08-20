package com.dla.foundation.analytics.utils;

import java.io.IOException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

public class CassandraContext {
	private Cluster cluster;
	private Session session;
	private String config_path;

	/**
	 * Initializes Cassandracontext with yaml configuration file path. Valid
	 * config path requires to start mock cassandra. Otherwise pass null value.
	 * 
	 * @param config_path
	 * @throws IOException
	 */
	public CassandraContext(String config_path) throws IOException {
		if (config_path != null && (!config_path.startsWith("file")))
			throw new IOException("Invalid file path");
		this.config_path = config_path;
	}

	/**
	 * This method will be used to start mock cassandra.
	 * 
	 * @throws InterruptedException
	 * @throws IOException
	 *             throws exception if tries to connect mock-cassandra with null
	 *             path.
	 */
	public void connect() throws InterruptedException, IOException {
		if (config_path == null)
			throw new IOException("Invalid file path");
		MockCassandra cassandra = new MockCassandra(config_path);
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
		cluster.shutdown();
	}

	/**
	 * This method will execute the specified commands. Befor this it should
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

	/**
	 * This method will get all rows for given keyspace and tablename by
	 * filtering over specified sparkAppName
	 * 
	 * @param keyspace
	 *            - a casandra keyspace
	 * @param table
	 *            - table name/column family
	 * @param sparkAppName
	 *            - sparkappname reading dynamic properties
	 * @return resultset
	 */
	public ResultSet getRows(String keyspace, String table, String sparkAppCF,
			String sparkAppName) {
		Clause clause = QueryBuilder.eq(sparkAppCF, sparkAppName);

		Where query = QueryBuilder.select().from(keyspace, table).where(clause);
		return session.execute(query);
	}
}