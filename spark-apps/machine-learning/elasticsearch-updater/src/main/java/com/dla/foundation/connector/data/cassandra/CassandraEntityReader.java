package com.dla.foundation.connector.data.cassandra;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

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
	public static Broadcast<ESWriter> bv = null;
	
	/*
	 * Reads dynamic and static properties using property handler
	 * Initializes cassandra connector with configuration like cassandra keyspace, ip, etc.
	 */
	
	public void runUserRecoDriver( String commonFilePath ) throws IOException, ParseException{
		cassandraESProp = new PropertiesHandler(commonFilePath,StaticProps.APP_NAME.getValue());
		cassandraSparkConnector= initializeCassandraConnector(commonFilePath);
		userRecoConfig= initializeCassandraConfig(commonFilePath);
		readData();
		/* After the reading-inserting job is done the input date stored as dynamic property
		 * in eo_spark_app_prop column family is increamented. 
		 */
		Date input_date = DateUtils.addDays(DateUtil.getDate(cassandraESProp.getValue(PropKeys.INPUT_DATE.getValue()),DATE_FORMAT), 1);

		cassandraESProp.writeToCassandra(StaticProps.INPUT_DATE
				.getValue(), DateUtil.getDateInFormat(input_date,DATE_FORMAT));
	 cassandraESProp.close();
	}

	private void readData() throws IOException, ParseException {
		JavaSparkContext sparkContext = new JavaSparkContext(cassandraESProp.getValue(PropKeys.MODE_PROPERTY.getValue()),StaticProps.APP_NAME.getValue());
		/*
		 * While running app in distributed mode, the worker nodes need to have ES specific info so 
		 * all the requiired things stored in a map and broadcasted here by master.
		 */
		Map<String,String> map = createMapToBeBroadcasted();
		Broadcast<Map<String,String>> bv = sparkContext.broadcast(map);
		JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD;
		Configuration conf= new Configuration();
	    
		/*
		 * Here the records with date equal to input date, stored as dynamic property, are fetched from cassandra 
		 * so used a filter clause here.
		 */
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
		
		logger.info("cassandraRDD count"+cassandraRDD.count());
		transformData(cassandraRDD,bv.value());
	}

	private Map<String,String> createMapToBeBroadcasted() {
		// TODO Auto-generated method stub
		Map<String,String> map= new HashMap<String,String>();
		map.put("esHost", ESWriter.esHost);
		logger.info("reco type from master"+ESWriter.reco_type.getActive() +ESWriter.reco_type.getPassive()+"buff threshold"+map.get("buffer_threshold"));
		map.put("PassiveRecoType",ESWriter.reco_type.getPassive());
		map.put("ActiveRecoType",ESWriter.reco_type.getActive());
		map.put("type",ESWriter.reco_type.getPassive());
		map.put("createIndex",String.valueOf(ESWriter.createIndex));
		map.put("schemaFilePath",ESWriter.schemaFilePath);
		map.put("buffer_threshold", String.valueOf(ESWriter.buffer_threshold));
		return map;
	}

	private void transformData(JavaPairRDD<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cassandraRDD, final Map<String,String> map) {
		 CassandraESTransformer transformer = new UserRecoTransformation(map);
		 JavaPairRDD<String, ESEntity> userEventRDD = transformer.extractEntity(cassandraRDD);
		 List<Tuple2<String, ESEntity>> lst=	userEventRDD.collect();
		 logger.info("RDD list size after collecting "+lst.size());
		 /*
		  * After insertion of records into passive reco type, it swaps the reco type. i.e 
		  * active becomes passive and passive becomes active.
		  */
		 ESWriter.swapRecoType();
	}
	
	
	
	private CassandraSparkConnector initializeCassandraConnector(String filePath) throws IOException{
		logger.info("Initializing cassandra connector");
		logger.info("cs hostlist "+cassandraESProp.getValue(PropKeys.INPUT_HOST_LIST.getValue()));
		cassandraSparkConnector = new CassandraSparkConnector(
				getList(cassandraESProp.getValue(PropKeys.INPUT_HOST_LIST.getValue()),IP_SEPARATOR),
				StaticProps.INPUT_PARTITIONER.getValue(),
				cassandraESProp.getValue(PropKeys.INPUT_RPC_PORT.getValue()),
				getList(cassandraESProp.getValue(PropKeys.OUTPUT_HOST_LIST.getValue()),","), StaticProps.OUTPUT_PARTITIONER.getValue());
		return cassandraSparkConnector;
	}
	

	private CassandraConfig initializeCassandraConfig(String userReco)throws IOException{
		logger.info("initializing cassandra config");
		logger.info("input keyspace"+cassandraESProp.getValue(PropKeys.INPUT_KEYSPACE.getValue()));
		logger.info("input column family"+StaticProps.INPUT_COLUMNFAMILY.getValue());
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
