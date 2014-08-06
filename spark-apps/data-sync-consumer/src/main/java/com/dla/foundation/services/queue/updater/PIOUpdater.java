package com.dla.foundation.services.queue.updater;

import io.prediction.Client;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;
import com.dla.foundation.analytics.utils.PropertiesHandler;

/**
 * PredictionIO Specific updater.
 * Used this updater when the data is to be written to PredictionIO.
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class PIOUpdater implements Updater {

	final Logger logger = Logger.getLogger(this.getClass());

	private Client client;
	private final String DEFAULT_PROPERTIES_FILE_PATH = "src/main/resources/PIO_props.properties";
	private final String PROPERTIES_FILE_VAR = "piopropertiesfile";
	private final int DEFAULT_API_PORT_NUM = 8000;

	private String propertiesFilePath;
	private PropertiesHandler phandler;
	private String hostname;
	private int port;
	private String appURL;
	private String appKey;

	public PIOUpdater() {
		propertiesFilePath = System.getProperty(PROPERTIES_FILE_VAR,DEFAULT_PROPERTIES_FILE_PATH);
		
		try {
			phandler = new PropertiesHandler(propertiesFilePath);
			hostname = phandler.getValue("hostname");
			appKey = phandler.getValue("appkey");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			port = (Integer.parseInt(phandler.getValue("port")) != -1) ? Integer.parseInt(phandler.getValue("port")) : DEFAULT_API_PORT_NUM;
		} catch (NumberFormatException | IOException e) {
			port = DEFAULT_API_PORT_NUM;
			logger.error(e.getMessage(), e);
		}

		appURL = "http://" + hostname + ":" + port;
		client = new Client(appKey, appURL);
	}

	@Override
	public void close() {
		client.close();
		logger.info("PredictionIO Client closed");
	}

	@Override
	public AnalyticsCollectionEvent updateSyncEvent(
			AnalyticsCollectionEvent event) {
		try {
			client.createUser(event.visitorProfileId);
			client.createItem(event.customEventValue, new String[]{"movie"});
			client.userActionItem(event.visitorProfileId, event.customEventAction, event.customEventValue);
		} catch (ExecutionException | InterruptedException | IOException e) {
			logger.error(e.getMessage(), e);
		}
		return null;
	}

	@Override
	public void updateAsyncEvent(AnalyticsCollectionEvent event) {
		try {
			client.createUser(event.visitorProfileId);
			client.createItem(event.customEventValue, new String[]{"movie"});
			client.userActionItem(event.visitorProfileId, event.customEventAction, event.customEventValue);
		} catch (ExecutionException | InterruptedException | IOException e) {
			logger.error(e.getMessage(), e);
		}
	}
}