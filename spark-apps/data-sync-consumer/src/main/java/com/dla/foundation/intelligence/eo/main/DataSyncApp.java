package com.dla.foundation.intelligence.eo.main;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;
import com.dla.foundation.intelligence.eo.consumer.RMQQueueReceiver;

/**
 * Driver app for spark based rabbitmq message consumers
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class DataSyncApp {

	final static Logger logger = Logger.getLogger(DataSyncApp.class);

	public static void main( String[] args)	{

		if(args.length < 1) {
			System.out.println("Expected argumets: [local | standalone <master> <spark_home> <jar_list_csv> | submit] ");
			System.exit(1);
		}

		String mode = args[0];
		String master = "local";
		String sparkHome = null;
		String[] jarList = null;
		JavaStreamingContext ctx = null;
		if(mode.equals("local")) {
			ctx = new JavaStreamingContext(master,"DataSyncConsumer", new Duration(1000));
		} else if(mode.equals("standalone")) {
			master = args[1];
			sparkHome = args[2];
			jarList = args[3].split(",");
			ctx = new JavaStreamingContext(master,"DataSyncConsumer", new Duration(1000),sparkHome,jarList);
		} else if(mode.equals("submit")) {
			ctx = new JavaStreamingContext(new SparkConf(),new Duration(1000));
		}

		try {
			JavaDStream<AnalyticsCollectionEvent> customReceiverStream = ctx.receiverStream(new RMQQueueReceiver(StorageLevel.MEMORY_AND_DISK()));
			customReceiverStream.print();	
			logger.info("Starting Spark Streaming Context");
			ctx.start();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
}