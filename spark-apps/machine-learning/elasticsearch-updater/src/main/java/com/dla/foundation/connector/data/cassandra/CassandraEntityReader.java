package com.dla.foundation.connector.data.cassandra;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
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
import com.dla.foundation.connector.util.DateUtil;
import com.dla.foundation.connector.util.PropKeys;
import com.dla.foundation.connector.util.StaticProps;

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
	PropertiesHandler cassandraESProp;
	public static String DATE_FORMAT ="yyyy-MM-dd";
	final private Logger logger = Logger.getLogger(CassandraEntityReader.class);
	public void runUserRecoDriver( String commonFilePath ) throws IOException, ParseException{
		cassandraESProp = new PropertiesHandler(commonFilePath,StaticProps.APP_NAME.getValue());
		cassandraSparkConnector= initializeCassandraConnector(commonFilePath);
		userRecoConfig= initializeCassandraConfig(commonFilePath);
		//ESWriter.init(esFilePath); //As already initialized at start of app
		readData();
		
		//Commenting for testing purpose
		
		Date input_date = DateUtils.addDays(
				DateUtil.getDate(cassandraESProp
						.getValue(PropKeys.INPUT_DATE.getValue()),
						DATE_FORMAT), 1);

		cassandraESProp.writeToCassandra(StaticProps.INPUT_DATE
				.getValue(), DateUtil.getDateInFormat(input_date,DATE_FORMAT));
	 cassandraESProp.close();
	}

	private void readData() throws IOException, ParseException {
		//PropertiesHandler appProp = new PropertiesHandler(filePath);
		JavaSparkContext sparkContext = new JavaSparkContext(StaticProps.SPARK_MODE.getValue(),StaticProps.APP_NAME.getValue());
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD;
		Configuration conf= new Configuration();
	    
		
		
		long day_timestamp = 	DateUtil.getDateInLong(cassandraESProp.getValue(StaticProps.INPUT_DATE.getValue()),DATE_FORMAT);
				//DateUtil.getPreviousDay();
		
        String filterClause =  "date = "+ day_timestamp;
        logger.info("filterClause" + filterClause);
		cassandraRDD = cassandraSparkConnector.read(conf, sparkContext,
				userRecoConfig.getInputKeyspace(),
				userRecoConfig.getInputColumnfamily(),
				userRecoConfig.getPageRowSize(), filterClause); 
		logger.info("InputKeyspace" + userRecoConfig.getInputKeyspace());
		logger.info("InputColumnfamily" + userRecoConfig.getInputColumnfamily());
		
		logger.info("cassandraRDD count"+cassandraRDD.count());;
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
		 ESWriter.swapRecoType();
		
		 
	}
	
	
	
	private CassandraSparkConnector initializeCassandraConnector(String filePath) throws IOException{
		logger.info("Initializing cassandra connector");
		
		cassandraSparkConnector = new CassandraSparkConnector(
				getList(cassandraESProp.getValue(PropKeys.INPUT_HOST_LIST.getValue()),IP_SEPARATOR),
				StaticProps.INPUT_PARTITIONER.getValue(),
				cassandraESProp.getValue(PropKeys.INPUT_RPC_PORT.getValue()),
				getList(cassandraESProp.getValue(PropKeys.OUTPUT_HOST_LIST.getValue()),","), StaticProps.OUTPUT_PARTITIONER.getValue());
		return cassandraSparkConnector;
	}
	

	private CassandraConfig initializeCassandraConfig(String userReco)throws IOException{
		logger.info("initializing cassandra config for  user Summary service");
		
		userRecoConfig = new CassandraConfig(
				cassandraESProp.getValue(PropKeys.INPUT_KEYSPACE.getValue()),
				null,
				StaticProps.INPUT_COLUMNFAMILY.getValue(),
				null, 
				cassandraESProp.getValue(PropKeys.PAGE_ROW_SIZE.getValue()), null);
		return userRecoConfig;
		
	}

	public static String[] getList(String value, String delimiter) {

		return value.split(delimiter);

	}
	
	
}
