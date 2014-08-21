package com.dla.foundation.services.queue.updater;

import io.prediction.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;

import com.dla.foundation.services.queue.filter.Filter;
import com.dla.foundation.services.queue.filter.FilterException;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.data.entities.analytics.UserEvent;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;

/**
 * PredictionIO Specific updater.
 * Used this updater when the data is to be written to PredictionIO.
 * 
 * @author tsudake.psl@dlavideo.com
 *
 */
public class PIOUpdater extends Updater {

	final Logger logger = Logger.getLogger(this.getClass());
	private Client client;
	private String PROPERTIES_FILE_NAME = "PIO_props.properties";
	private String PROPERTIES_FILE_VAR = "piopropertiesfile";
	private String propertiesFilePath = System.getProperty(PROPERTIES_FILE_VAR);
	private final int DEFAULT_API_PORT_NUM = 8000;

	private PropertiesHandler phandler;
	private String hostname;
	private int port;
	private String appURL;
	private String appKey;

	public PIOUpdater() {

		if(propertiesFilePath == null)
			propertiesFilePath = SparkFiles.get(PROPERTIES_FILE_NAME);

		try {
			phandler = new PropertiesHandler(propertiesFilePath);
			hostname = phandler.getValue("hostname");
			try {
				port = (Integer.parseInt(phandler.getValue("port")) != -1) ? Integer.parseInt(phandler.getValue("port")) : DEFAULT_API_PORT_NUM;
			} catch (NumberFormatException e) {
				port = DEFAULT_API_PORT_NUM;
				logger.error(e.getMessage(), e);
			}
			appURL = "http://" + hostname + ":" + port;
			appKey = phandler.getValue("appkey");
			client = new Client(appKey, appURL);
		} catch (IOException e1) {
			logger.error(e1.getMessage(), e1);
		}
	}

	@Override
	protected <TEntity extends SimpleFoundationEntity> TEntity filterEvent(
			TEntity event, ArrayList<Filter> filters) throws FilterException {
		for (Filter filter : filters) {
			event = filter.doFilter(event);
		}
		return event;
	}

	@Override
	protected <TEntity extends SimpleFoundationEntity> TEntity doUpdateSyncEvent(
			TEntity event) {
		//		try {
		//		client.createUser(event.visitorProfileId);
		//		client.createItem(event.customEventValue, new String[]{"movie"});
		//		client.userActionItem(event.visitorProfileId, event.customEventAction, event.customEventValue);
		//	} catch (ExecutionException | InterruptedException | IOException e) {
		//		logger.error(e.getMessage(), e);
		//	}
		return null;
	}

	@Override
	protected <TEntity extends SimpleFoundationEntity> void doUpdateAsyncEvent(
			TEntity event) {
		//		try {
		//			client.createUser(event.visitorProfileId);
		//			client.createItem(event.customEventValue, new String[]{"movie"});
		//			client.userActionItem(event.visitorProfileId, event.customEventAction, event.customEventValue);
		//		} catch (ExecutionException | InterruptedException | IOException e) {
		//			logger.error(e.getMessage(), e);
		//		}

	}

	@Override
	public void close() {
		client.close();
		logger.info("PredictionIO Client closed");
	}
}