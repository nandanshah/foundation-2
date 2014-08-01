package com.dla.foundation;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;

public class CassandraWriter {

	private JobConf conf;
	private String[] cassandraIP;
	private String columnFamilyPartitioner;
	private String port;
	private String keySpace;
	private String columnFamily;
	private String primaryKey;
	private String updateQuery;
	private String recoField;
	private CassandraSparkConnector con;

	public CassandraWriter(JobConf conf, String[] cassandraIP, String port,
			String keyspace, String columnFamily, String primaryKey,
			String columnFamilyPartitioner, String recoField) {

		this.conf = conf;
		this.cassandraIP = cassandraIP;
		this.port = port;
		this.keySpace = keyspace;
		this.columnFamily = columnFamily;
		this.primaryKey = primaryKey;
		this.columnFamilyPartitioner = columnFamilyPartitioner;
		this.recoField = recoField;
		con = new CassandraSparkConnector(this.cassandraIP,
				this.columnFamilyPartitioner, this.port, this.cassandraIP,
				this.columnFamilyPartitioner);
		updateQuery = "Update " + keySpace + "." + columnFamily + " SET "
				+ this.recoField + "=?";
	}

	public void writeToCassandra(
			JavaPairRDD<Map<String, ByteBuffer>, List<ByteBuffer>> cassandraRDD) {
		con.write(conf, this.keySpace, this.columnFamily,
				this.updateQuery,cassandraRDD);

	}
}
