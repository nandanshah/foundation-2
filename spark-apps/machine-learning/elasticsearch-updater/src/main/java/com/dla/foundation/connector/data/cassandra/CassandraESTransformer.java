package com.dla.foundation.connector.data.cassandra;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;

import com.dla.foundation.connector.model.ESEntity;

public interface CassandraESTransformer {

	 JavaPairRDD<String, ESEntity> extractEntity(JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD );
}
