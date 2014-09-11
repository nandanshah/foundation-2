package com.dla.foundation.data.persistence.elasticsearch;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.dla.foundation.analytics.utils.CommonPropKeys;
import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;
import com.dla.foundation.services.contentdiscovery.entities.MediaItem;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BulkEventsProcessing {

	private static StringBuilder bulkEvents = new StringBuilder();
	private static int count=0;
	private final static int buffer_threshold = 5;
	private PropertiesHandler phandler= null;
	private static String index, movieType, userRecoType;
	final private Logger logger = Logger.getLogger(BulkEventsProcessing.class);

	public BulkEventsProcessing(PropertiesHandler handler){
		phandler= handler;
		try {
			index = phandler.getValue(CommonPropKeys.es_index_name);
			movieType = phandler.getValue(CommonPropKeys.es_movie_index_type);
			userRecoType = phandler.getValue(CommonPropKeys.es_userreco_index_type);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}

	}

	public void getBulkEvent(AnalyticsCollectionEvent event, ESService es_service){
		if(Integer.compare(buffer_threshold, count)==0){
			logger.info("Posting events"+bulkEvents.toString());
			es_service.postBulkEvents(bulkEvents.toString());
		}
		else{
			count++;
			try {
				processEvent(event);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	public void processEvent(AnalyticsCollectionEvent event) throws Exception {
		JSONObject obj=null;
		MediaItem movieItem= new MediaItem();
		UserRecommendation user_reco= new UserRecommendation();
		ObjectMapper mapper = new ObjectMapper();
		String entityJson= null;
		if(event.customEventLabel.equals(phandler.getValue("movie.add"))){	
			obj= getObj(index, movieType, event.customEventValue, "create", null);
			movieItem.id=event.customEventValue;
			entityJson = mapper.writeValueAsString(movieItem);
		}
		if(event.customEventLabel.equals(phandler.getValue("userReco.add"))){	
			obj= getObj(index, userRecoType, event.customEventValue, "create", event.linkId);
			user_reco.userid= event.customEventValue;
			entityJson = mapper.writeValueAsString(user_reco);
		}
		if(event.customEventLabel.equals(phandler.getValue("movie.update"))){
			obj= getObj(index, movieType, event.customEventValue, "update", null);
			entityJson= getUpdateObj("title", event.customMetric).toString();
		}
		if(event.customEventLabel.equals(phandler.getValue("userReco.update"))){
			obj= getObj(index, userRecoType, event.customEventValue, "update", event.linkId);
			entityJson= getUpdateObj("userid", event.customMetric).toString();
		}
		if(event.customEventLabel.equals(phandler.getValue("movie.delete"))){
			obj= getObj(index, movieType, event.customEventValue, "delete", null);
		}
		if(event.customEventLabel.equals(phandler.getValue("userReco.delete"))){
			obj= getObj(index, userRecoType, event.customEventValue, "delete", event.linkId);
		}
		if(obj!=null & entityJson!=null)
			bulkEvents.append(obj.toString()+"\n"+entityJson+"\n");
		else if(obj != null)
			bulkEvents.append(obj.toString()+"\n"); 
		logger.debug("Bulk async events to be posted "+bulkEvents);
	}

	private JSONObject getObj(String indexName, String type, String id, String operType, String parentId){
		JSONObject obj= new JSONObject();
		obj.put("_index", indexName);
		obj.put("_type", type);
		obj.put("_id", id);
		if(parentId!=null)
			obj.put("_parent", parentId);

		JSONObject indexobj= new JSONObject();
		indexobj.put(operType, obj);
		return indexobj;
	}

	private JSONObject getUpdateObj(String key, String value){
		JSONObject dataobj= new JSONObject();
		dataobj.put(key, value);
		JSONObject updateobj= new JSONObject();
		updateobj.put("doc", dataobj);
		return updateobj;
	}
}
