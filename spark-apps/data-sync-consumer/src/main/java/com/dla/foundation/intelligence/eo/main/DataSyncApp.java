package com.dla.foundation.intelligence.eo.main;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.dla.foundation.data.persistence.SimpleFoundationEntity;
import com.dla.foundation.intelligence.eo.consumer.RMQQueueReceiver;

/**
 * Driver app for spark based rabbitmq message consumers
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class DataSyncApp {

	private final static Logger logger = Logger.getLogger(DataSyncApp.class);
	private final static String appName = "DataSyncConsumer";
	private final static long batchDuration = 1000;

	public static void main( String[] args)	{
		
		if(args.length < 1) {
			System.out.println("Expected argumets: [local | standalone <master> <spark_home> <jar_list_csv> | submit] ");
			System.exit(1);
		}

		String mode = args[0];
		String master = "local";
		String sparkHome = null;
		String[] jarList = null;
		JavaSparkContext sparkCtx = null;
		
		if(mode.equals("local")) {
			sparkCtx = new JavaSparkContext(master, appName);
		} 
		else if(mode.equals("standalone")) {
			master = args[1];
			sparkHome = args[2];
			jarList = args[3].split(",");
			sparkCtx = new JavaSparkContext(master, appName, sparkHome, jarList);
		} else if(mode.equals("submit")) {
			sparkCtx = new JavaSparkContext(new SparkConf());
		}

		try {
			JavaStreamingContext sparkStreamingCtx = new JavaStreamingContext(sparkCtx, new Duration(batchDuration));
			JavaDStream<SimpleFoundationEntity> customReceiverStream = sparkStreamingCtx.receiverStream(new RMQQueueReceiver(StorageLevel.MEMORY_AND_DISK()));
			customReceiverStream.print();

			//Add Shutdown Hook
			Runtime.getRuntime().addShutdownHook(new SIGTERMHandler(sparkStreamingCtx));

			logger.info("Starting Spark Streaming Context");
			sparkStreamingCtx.start();
			sparkStreamingCtx.awaitTermination();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
}

class SIGTERMHandler extends Thread {

	private final static Logger logger = Logger.getLogger(DataSyncApp.class);
	private JavaStreamingContext streamingContext;

	public SIGTERMHandler(final JavaStreamingContext streamingContext) {
		this.streamingContext = streamingContext;
	}

	@Override
	public void run() {
		logger.info("Shutting Down Data Sync Consumer App...");
		super.run();
		streamingContext.stop(false, true);
	}
}