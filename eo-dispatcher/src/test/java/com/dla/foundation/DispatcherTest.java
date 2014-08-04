package com.dla.foundation;

import java.io.IOException;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import com.dla.foundation.fis.eo.dispatcher.RabbitMQAMQPConnector;
import com.dla.foundation.fis.eo.entities.DeviceType;
import com.dla.foundation.fis.eo.entities.EOConfig;
import com.dla.foundation.fis.eo.exception.DispatcherException;

public class DispatcherTest {

	private RabbitMQAMQPConnector rmqC;

	@Before
	public void before() {
		rmqC = new RabbitMQAMQPConnector();
		try {
			rmqC.init(new EOConfig());
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
			try {
				boolean success = rmqC.accountAdd(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), System.currentTimeMillis(), UUID.randomUUID(), UUID.randomUUID(), DeviceType.ANDROID_PHONE, UUID.randomUUID());
				assertEquals(true, success);
			} catch (DispatcherException e) {

			}
		}
	}
}
