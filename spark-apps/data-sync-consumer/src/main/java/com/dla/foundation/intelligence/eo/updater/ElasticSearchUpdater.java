package com.dla.foundation.intelligence.eo.updater;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;
import com.dla.foundation.data.persistence.elasticsearch.BulkEventsProcessing;
import com.dla.foundation.data.persistence.elasticsearch.ESService;
import com.dla.foundation.data.persistence.elasticsearch.ESServiceImpl;
import com.dla.foundation.intelligence.eo.filter.Filter;
import com.dla.foundation.intelligence.eo.filter.FilterException;

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
	protected <TEntity extends SimpleFoundationEntity> void doUpdateAsyncEvent(
			TEntity event) {
		//		bulk_events.getBulkEvent(event, es_service);
	}

	@Override
	public void close() {

	}
}