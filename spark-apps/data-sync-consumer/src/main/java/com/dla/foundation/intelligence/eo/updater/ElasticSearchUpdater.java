package com.dla.foundation.intelligence.eo.updater;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;
import org.json.JSONArray;
import org.json.JSONObject;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.data.entities.event.Event;
import com.dla.foundation.data.entities.event.EventType;
import com.dla.foundation.data.persistence.SimpleFoundationEntity;
import com.dla.foundation.data.persistence.elasticsearch.ESService;
import com.dla.foundation.data.persistence.elasticsearch.ESServiceImpl;
import com.dla.foundation.intelligence.eo.filter.Filter;
import com.dla.foundation.intelligence.eo.filter.FilterException;

public class ElasticSearchUpdater extends Updater {

	private static ESService es_service=null;

	private static ElasticSearchUpdater instance;

	private final String PROPERTIES_FILE_NAME = "common.properties";
	private final String PROPERTIES_FILE_VAR = "commonproperties";
	final Logger logger = Logger.getLogger(this.getClass());
	private String propertiesFilePath = System.getProperty(PROPERTIES_FILE_VAR);
	private PropertiesHandler phandler= null;

	private JSONObject filterJson = new JSONObject();

	private ElasticSearchUpdater() throws Exception {
		if(propertiesFilePath == null)
			propertiesFilePath = SparkFiles.get(PROPERTIES_FILE_NAME);
		phandler= new PropertiesHandler(propertiesFilePath);
		es_service = new ESServiceImpl(phandler);
	}

	public static ElasticSearchUpdater getInstance() throws Exception {
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
		Event e = (Event) event;
		if(e.eventType==EventType.ProfileDeleted) {
			prepareFilter(e.visitorProfileId);
			es_service.deleteUserReco(filterJson.toString());
		}
	}

	@Override
	public void close() {

	}

	private void prepareFilter(String userId) {
		filterJson = new JSONObject();
		filterJson.put("query", new JSONObject().put("bool", new JSONObject().put(
				"must", new JSONArray().put(new JSONObject().put(
						"match", new JSONObject().put("enabled", 1))))));
		logger.info(filterJson.toString());
		JSONObject userIdJ = new JSONObject().put("match", new JSONObject().put("profileId", userId));
		filterJson.getJSONObject("query").getJSONObject("bool").getJSONArray("must").put(userIdJ);
	}
}