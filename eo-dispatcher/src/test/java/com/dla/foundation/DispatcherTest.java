package com.dla.foundation;

import java.io.IOException;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

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
		for(int i = 0; i < 50; i++) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			try {
				rmqC.accountAdd(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), System.currentTimeMillis(), UUID.randomUUID(), UUID.randomUUID(), DeviceType.ANDROID_PHONE, UUID.randomUUID());
			} catch (DispatcherException e) {

			}
		}
	}
}
