package com.dla.foundation.services.queue.updater;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.data.entities.analytics.UserEvent;
import com.dla.foundation.data.persistence.elasticsearch.BulkEventsProcessing;
import com.dla.foundation.data.persistence.elasticsearch.ESService;
import com.dla.foundation.data.persistence.elasticsearch.ESServiceImpl;
import com.dla.foundation.services.queue.filter.Filter;

public class ElasticSearchUpdater extends Updater {

	private static ESService es_service=null;
	private static BulkEventsProcessing bulk_events=null;

	private static ElasticSearchUpdater instance;

	private final String PROPERTIES_FILE_NAME = "ElasticSearch.properties";
	private final String PROPERTIES_FILE_VAR = "espropertiesfile";
	final Logger logger = Logger.getLogger(this.getClass());
	String propertiesFilePath = System.getProperty(PROPERTIES_FILE_VAR);
	PropertiesHandler phandler= null;

	private ElasticSearchUpdater()
	{	
		if(propertiesFilePath == null)
			propertiesFilePath = SparkFiles.get(PROPERTIES_FILE_NAME);

		try {
			phandler= new PropertiesHandler(propertiesFilePath);
			es_service = new ESServiceImpl(phandler);
			bulk_events= new BulkEventsProcessing(phandler);
		} catch (IOException e) {
			logger.error("IOException in updater");
		}
	}

	public static ElasticSearchUpdater getInstance() {
		if(instance==null) {
			instance = new ElasticSearchUpdater();
		}
		return instance;
	}

	@Override
	protected void filterEvent(UserEvent event,
			ArrayList<Filter> filters) {
		// TODO Auto-generated method stub
	}

	@Override
	protected UserEvent doUpdateSyncEvent(
			UserEvent event) {
		//		try{
		//			if(event.customEventLabel.contains("Added"))
		//				es_service.addItem(event);
		//			if(event.customEventLabel.contains("Updated"))
		//				es_service.updateItem(event);
		//			if(event.customEventLabel.contains("Deleted"))
		//				es_service.deleteItem(event);
		//		}
		//		catch(IOException e){
		//			logger.error("IOException in ES update sync events");
		//		}
		return event;
	}

	@Override
	protected void doUpdateAsyncEvent(UserEvent event) {
		//		bulk_events.getBulkEvent(event, es_service);
	}

	@Override
	public void close() {

	}
}