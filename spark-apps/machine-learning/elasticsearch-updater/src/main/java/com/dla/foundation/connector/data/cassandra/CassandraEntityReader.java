package com.dla.foundation.connector.data.cassandra;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.dla.foundation.analytics.utils.CassandraSparkConnector;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.connector.model.CassandraConfig;
import com.dla.foundation.connector.model.ESEntity;
import com.dla.foundation.connector.persistence.elasticsearch.ESWriter;
import com.dla.foundation.connector.util.PropKeys;


public class CassandraEntityReader implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2981888233197323476L;
	private static final String IP_SEPARATOR = ",";
	final Logger logger = Logger.getLogger(this.getClass());
	CassandraConfig userRecoConfig;
	CassandraSparkConnector cassandraSparkConnector;
	
	public void runUserRecoDriver( String sparkFilePath, String userRecoFilePath, String esFilePath ) throws IOException{
		cassandraSparkConnector= initializeCassandraConnector(sparkFilePath);
		userRecoConfig= initializeCassandraConfig(userRecoFilePath);
		ESWriter.init(esFilePath);
		readData(sparkFilePath);
		
		
	}

	private void readData(String filePath) throws IOException {
		PropertiesHandler appProp = new PropertiesHandler(filePath);
		JavaSparkContext sparkContext = new JavaSparkContext(appProp.getValue("spark.mode"),appProp.getValue("app.name"));
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD;
		Configuration conf= new Configuration();
		cassandraRDD = cassandraSparkConnector.read(conf, sparkContext,
				userRecoConfig.getInputKeyspace(),
				userRecoConfig.getInputColumnfamily(),
				userRecoConfig.getPageRowSize());
		transformData(cassandraRDD);
	}

	private void transformData(JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {
		 CassandraESTransformer transformer = new UserRecoTransformation();
		 JavaPairRDD<String, ESEntity> userEventRDD = transformer.extractEntity(cassandraRDD);
		 List<Tuple2<String, ESEntity>> lst=	userEventRDD.collect();
	}
	
	
	
	private CassandraSparkConnector initializeCassandraConnector(String filePath) throws IOException{
		logger.info("Initializing cassandra connector");
		PropertiesHandler appProp = new PropertiesHandler(filePath);
		cassandraSparkConnector = new CassandraSparkConnector(
				getList(appProp.getValue(PropKeys.INPUT_HOST_LIST.getValue()),IP_SEPARATOR),
				appProp.getValue(PropKeys.INPUT_PARTITIONER.getValue()),
				appProp.getValue(PropKeys.INPUT_RPC_PORT.getValue()),
				getList(appProp.getValue(PropKeys.OUTPUT_HOST_LIST.getValue()),","), appProp.getValue(PropKeys.OUTPUT_PARTITIONER.getValue()));
		return cassandraSparkConnector;
	}
	

	private CassandraConfig initializeCassandraConfig(String userReco)throws IOException{
		logger.info("initializing cassandra config for  user Summary service");
		PropertiesHandler userRecoProp = new PropertiesHandler(userReco);
		userRecoConfig = new CassandraConfig(
				userRecoProp.getValue(PropKeys.INPUT_KEYSPACE.getValue()),
				null,
				userRecoProp.getValue(PropKeys.INPUT_COLUMNFAMILY.getValue()),
				null, 
				userRecoProp.getValue(PropKeys.PAGE_ROW_SIZE.getValue()), null);
		return userRecoConfig;
		
	}

	public static String[] getList(String value, String delimiter) {

		return value.split(delimiter);

	}
}
