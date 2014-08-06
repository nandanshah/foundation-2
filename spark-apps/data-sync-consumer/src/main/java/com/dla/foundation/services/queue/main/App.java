package com.dla.foundation.services.queue.main;

import com.dla.foundation.services.queue.consumer.SparkConsumerAMQP;

/**
 * Driver app for spark based rabbitmq message consumers
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class App {	
	public static void main( String[] args)	{
		Thread t = new Thread(new SparkConsumerAMQP());
		t.run();

		try {
			Thread.sleep(5000);
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
