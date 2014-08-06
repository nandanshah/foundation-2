package com.dla.foundation.services.queue.updater;

import java.io.IOException;

import org.apache.log4j.Logger;


import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;
import com.dla.foundation.data.persistence.elasticsearch.BulkEventsProcessing;
import com.dla.foundation.data.persistence.elasticsearch.ESService;
import com.dla.foundation.data.persistence.elasticsearch.ESServiceImpl;

public class ElasticSearchUpdater implements Updater {
	
	private static ESService es_service=null;
	private static BulkEventsProcessing bulk_events=null;
	
	private final String PROPERTIES_FILE_PATH = "src/main/resources/ElasticSearch.properties";
	private final String PROPERTIES_FILE_VAR = "espropertiesfile";
	final Logger logger = Logger.getLogger(this.getClass());
	String propertiesFilePath = System.getProperty(PROPERTIES_FILE_VAR,PROPERTIES_FILE_PATH);
	PropertiesHandler phandler= null;
	
	public ElasticSearchUpdater()
	{	try {
			phandler= new PropertiesHandler(propertiesFilePath);
			es_service = new ESServiceImpl(phandler);
			bulk_events= new BulkEventsProcessing(phandler);
		} catch (IOException e) {
			logger.error("IOException in updater");
		}
	}
	
	@Override
	public void close() {
	
	}

	@Override
	public AnalyticsCollectionEvent updateSyncEvent(AnalyticsCollectionEvent event) {
		try{
			if(event.customEventLabel.contains("Added"))
				es_service.addItem(event);
			if(event.customEventLabel.contains("Updated"))
				es_service.updateItem(event);
			if(event.customEventLabel.contains("Deleted"))
				es_service.deleteItem(event);
		}
		catch(IOException e){
			logger.error("IOException in ES update sync events");
		}
		return event;
	}

	@Override
	public void updateAsyncEvent(AnalyticsCollectionEvent event) {
		bulk_events.getBulkEvent(event, es_service);
	}
	
	
}
