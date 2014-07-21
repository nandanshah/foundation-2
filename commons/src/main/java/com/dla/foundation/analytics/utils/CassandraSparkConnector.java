package com.dla.foundation.analytics.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Generic utility to read and write data to and from Cassandra.
 * 
 * @author shishir_shivhare
 * @version 1.0
 * @since 2014-06-16
 * 
 */
public class CassandraSparkConnector {

	private final Map<String, ByteBuffer> keyClass = new LinkedHashMap<String, ByteBuffer>();
	private final Map<String, ByteBuffer> valueClass = new LinkedHashMap<String, ByteBuffer>();
	private final java.util.List<ByteBuffer> columnClass = new ArrayList<ByteBuffer>();

	private final String[] inputColumnfamilyIpAddress;
	private final String inputColumnFamilyPartitioner, port;
	private final String[] outputColumnfamilyIpAddress;
	private final String outputColumnFamilypartitioner;

	/**
	 * This function will set cassandra properties in conf through config
	 * helper.
	 * 
	 * @param conf
	 *            : Configuration to set properties related to cassandra.
	 * @param inputColumnfamilyIpAddress
	 *            : Ip address with which cassandra service is going to interact
	 *            while reading from columnfamily.
	 * @param inputColumnFamilyPartitioner
	 *            : Partitioner of the nodes to which cassandra service will
	 *            interact while reading from columnfamily.
	 * @param port
	 *            : input rpc port (9160) port on which cassandra service will
	 *            connect to cassandra.
	 * @param outputColumnfamilyIpAddress
	 *            : Ip address with which cassandra service is going to interact
	 *            while writing to columnfamily.
	 * @param outputColumnFamilypartitioner
	 *            : Partitioner of the nodes to which cassandra service will
	 *            interact while writing to columnfamily.
	 */
	public CassandraSparkConnector(String[] inputColumnfamilyIpAddress,
			String inputColumnFamilyPartitioner, String port,
			String[] outputColumnfamilyIpAddress,
			String outputColumnFamilypartitioner) {
		this.inputColumnfamilyIpAddress = inputColumnfamilyIpAddress;
		this.inputColumnFamilyPartitioner = inputColumnFamilyPartitioner;
		this.port = port;
		this.outputColumnfamilyIpAddress = outputColumnfamilyIpAddress;
		this.outputColumnFamilypartitioner = outputColumnFamilypartitioner;
	}

	private void initializeConf(Configuration conf) {
		ConfigHelper.setInputRpcPort(conf, port);
		ConfigHelper.setInputPartitioner(conf, inputColumnFamilyPartitioner);
		ConfigHelper.setOutputPartitioner(conf, outputColumnFamilypartitioner);
		ConfigHelper
				.setInputInitialAddress(conf, inputColumnfamilyIpAddress[0]);

		ConfigHelper.setOutputInitialAddress(conf,
				outputColumnfamilyIpAddress[0]);
	}

	/**
	 * This function will read the data from cassandra and return in Java pair
	 * RDD having key as maps and values map. First map contains the primary
	 * keys, specified while creating the schema of columnfamily as map's key
	 * and their value as map's value in bytebuffer. Second map contains other
	 * columns which are not specified in primary key while creating the schema
	 * for columnfamily as map's key and their value as map's value in
	 * bytebuffer.
	 * 
	 * @param conf
	 *            : Configuration to set properties related to cassandra.
	 * @param sparkContext
	 *            : spark context to interact with cassandra.
	 * @param keySpace
	 *            : Keyspace in which it will look for columnfamily to read
	 *            data.
	 * @param columnfamily
	 *            : Columnfamily from which it will read the data.
	 * @param pageRowSize
	 *            :number of rows per page.
	 * @return :Java Pair RDD having key as map string and bytebuffer and values
	 *         as map string and bytebuffer.
	 * 
	 */
	public JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> read(
			Configuration conf, JavaSparkContext sparkContext, String keySpace,
			String columnfamily, String pageRowSize) {
		initializeConf(conf);
		ConfigHelper.setInputColumnFamily(conf, keySpace, columnfamily);
		CqlConfigHelper.setInputCQLPageRowSize(conf, pageRowSize);

		@SuppressWarnings("unchecked")
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD = (JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>>) sparkContext
				.newAPIHadoopRDD(conf, CqlPagingInputFormat.class,
						keyClass.getClass(), valueClass.getClass());

		return cassandraRDD;

	}

	/**
	 * This function will read the data from cassandra and return a Java pair
	 * RDD having key and values map. First map contains the primary keys,
	 * specified while creating the schema of columnfamily as map's key and
	 * their value as map's value in bytebuffer. Second map contains other
	 * columns which are not specified in primary key while creating the schema
	 * for columnfamily as map's key and their value as map's value in
	 * bytebuffer.Additionally, it has provision to specify the where condition
	 * for data
	 * 
	 * @param conf
	 *            : Configuration to set properties related to cassandra.
	 * @param sparkContext
	 *            : spark context to interact with cassandra.
	 * @param keySpace
	 *            : Keyspace in which it will look for columnfamily to read
	 *            data.
	 * @param columnfamily
	 *            : Columnfamily from which it will read the data.
	 * @param pageRowSize
	 *            :number of rows per page.
	 * @param whereCondtion
	 *            : It will have where condition through which it will fetch
	 *            only data which agrees to where condition. Where clause only
	 *            works for = and < operator.
	 * @return :Java Pair RDD having key as map string and bytebuffer and values
	 *         as map string and bytebuffer.
	 * 
	 */

	public JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> read(
			Configuration conf, JavaSparkContext sparkContext, String keySpace,
			String columnfamily, String pageRowSize, String whereCondtion) {
		initializeConf(conf);
		ConfigHelper.setInputColumnFamily(conf, keySpace, columnfamily);
		CqlConfigHelper.setInputCQLPageRowSize(conf, pageRowSize);
		CqlConfigHelper.setInputWhereClauses(conf, whereCondtion);

		@SuppressWarnings("unchecked")
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD = (JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>>) sparkContext
				.newAPIHadoopRDD(conf, CqlPagingInputFormat.class,
						keyClass.getClass(), valueClass.getClass());

		return cassandraRDD;

	}

	/**
	 * This function writes data into specified Cassandra columnfamily.
	 * 
	 * @param conf
	 *            : Configuration to set properties related to cassandra.
	 * @param keySpace
	 *            : Keyspace in which it will look for columnfamily to write
	 *            data.
	 * @param columnfamily
	 *            : Columnfamily to which it will write the data.
	 * @param query
	 *            : It requires update query to write data. It will
	 *            automatically add where clause with the primary keys defined
	 *            for the columnfamily.
	 * @param cassandraRDD
	 *            : It requires Java pair RDD which will have key as map with
	 *            and values as list. Map will have all the keys which are
	 *            primary keys of columnfamily with its vlues in bytebuffer. If
	 *            columnfamily has id,name as primary key then map will have id
	 *            and its value (bytebuffer),name and its value(bytebuffer).
	 *            List will have all the column's value which are provided in
	 *            update query to set. Sequence will be same as specified in
	 *            update query.
	 */

	public void write(
			Configuration conf,
			String keySpace,
			String columnfamily,
			String query,
			JavaPairRDD<Map<String, ByteBuffer>, java.util.List<ByteBuffer>> cassandraRDD) {
		initializeConf(conf);
		ConfigHelper.setOutputKeyspace(conf, keySpace);
		ConfigHelper.setOutputColumnFamily(conf, columnfamily);
		CqlConfigHelper.setOutputCql(conf, query);
		cassandraRDD.saveAsNewAPIHadoopFile(keySpace, keyClass.getClass(),
				columnClass.getClass(), CqlOutputFormat.class, conf);

	}

}
