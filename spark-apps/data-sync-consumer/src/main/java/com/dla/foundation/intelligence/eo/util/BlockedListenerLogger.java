package com.dla.foundation.intelligence.eo.util;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.rabbitmq.client.BlockedListener;

public class BlockedListenerLogger implements BlockedListener {

	final Logger logger = Logger.getLogger(this.getClass());

	@Override
	public void handleBlocked(String arg0) throws IOException {
		logger.error("Connection blocked due to the broker running low on resources (memory or disk)");
		logger.error(arg0);
	}

	@Override
	public void handleUnblocked() throws IOException {
		logger.info("Connection unblocked, sufficient resources are available now");
	}

}
