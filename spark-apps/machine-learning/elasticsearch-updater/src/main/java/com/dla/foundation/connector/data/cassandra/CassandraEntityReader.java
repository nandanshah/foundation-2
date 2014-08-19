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
import com.dla.foundation.connector.util.CassandraKeys;
import com.dla.foundation.connector.util.DateUtil;

/*
 *  This class is responsible for reading user reco records from Cassandra table
 *  
 *  @author neha_jain
 */

public class CassandraEntityReader implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2981888233197323476L;
	private static final String IP_SEPARATOR = ",";
	CassandraConfig userRecoConfig;
	CassandraSparkConnector cassandraSparkConnector;
	final private Logger logger = Logger.getLogger(CassandraEntityReader.class);
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
	
		long day_timestamp = DateUtil.getPreviousDay();
        String filterClause =  "flag =1 and date = "+ day_timestamp;
        
		cassandraRDD = cassandraSparkConnector.read(conf, sparkContext,
				userRecoConfig.getInputKeyspace(),
				userRecoConfig.getInputColumnfamily(),
				userRecoConfig.getPageRowSize(), filterClause); 
		transformData(cassandraRDD);
	}

	private void transformData(JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD) {
		 CassandraESTransformer transformer = new UserRecoTransformation();
		 JavaPairRDD<String, ESEntity> userEventRDD = transformer.extractEntity(cassandraRDD);
		 List<Tuple2<String, ESEntity>> lst=	userEventRDD.collect();
		 
		 if (ESWriter.bulkEvents !=null && ESWriter.bulkEvents.length()>0){	
			 logger.info("Writing remaining records to ES");
			 ESWriter writer= new ESWriter();
			 writer.postBulkData(ESWriter.bulkEvents.toString());
		 }
		 System.out.println("Size"+lst.size());
		 
	}
	
	
	
	private CassandraSparkConnector initializeCassandraConnector(String filePath) throws IOException{
		logger.info("Initializing cassandra connector");
		PropertiesHandler appProp = new PropertiesHandler(filePath);
		cassandraSparkConnector = new CassandraSparkConnector(
				getList(appProp.getValue(CassandraKeys.INPUT_HOST_LIST.getValue()),IP_SEPARATOR),
				appProp.getValue(CassandraKeys.INPUT_PARTITIONER.getValue()),
				appProp.getValue(CassandraKeys.INPUT_RPC_PORT.getValue()),
				getList(appProp.getValue(CassandraKeys.OUTPUT_HOST_LIST.getValue()),","), appProp.getValue(CassandraKeys.OUTPUT_PARTITIONER.getValue()));
		return cassandraSparkConnector;
	}
	

	private CassandraConfig initializeCassandraConfig(String userReco)throws IOException{
		logger.info("initializing cassandra config for  user Summary service");
		PropertiesHandler userRecoProp = new PropertiesHandler(userReco);
		userRecoConfig = new CassandraConfig(
				userRecoProp.getValue(CassandraKeys.INPUT_KEYSPACE.getValue()),
				null,
				userRecoProp.getValue(CassandraKeys.INPUT_COLUMNFAMILY.getValue()),
				null, 
				userRecoProp.getValue(CassandraKeys.PAGE_ROW_SIZE.getValue()), null);
		return userRecoConfig;
		
	}

	public static String[] getList(String value, String delimiter) {

		return value.split(delimiter);

	}
	
	
}
