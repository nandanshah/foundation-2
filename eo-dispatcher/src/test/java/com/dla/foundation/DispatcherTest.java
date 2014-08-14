package com.dla.foundation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.dla.foundation.fis.eo.dispatcher.EOConfig;
import com.dla.foundation.fis.eo.dispatcher.RabbitMQDispatcher;
import com.dla.foundation.fis.eo.entities.Event;
import com.dla.foundation.fis.eo.exception.DispatcherException;

public class DispatcherTest {

	private RabbitMQDispatcher rmqC;

	@Before
	public void before() throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException {
		rmqC = new RabbitMQDispatcher();
		Properties props = new Properties();
		try {
			FileInputStream input = new FileInputStream("src/test/resources/RabbitMQ.properties");
			props.load(input);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			rmqC.init(new EOConfig(props.getProperty("rabbitmq_server_host"),Integer.parseInt(props.getProperty("rabbitmq_server_port"))));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testAddSyncEvent() {
		for(int i = 0; i < 500; i++) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			Event e = new Event();
			e.hitType = "profileAdded";
			e.accountId = UUID.randomUUID().toString();

			try {
				boolean success = rmqC.dispatchAsync(e);
				assertEquals(true, success);
			} catch (DispatcherException e1) {
				fail(e1.getMessage());
			}
		}
	}
}
