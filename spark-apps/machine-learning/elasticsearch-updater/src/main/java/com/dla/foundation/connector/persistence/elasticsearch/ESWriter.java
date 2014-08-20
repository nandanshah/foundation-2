package com.dla.foundation.connector.persistence.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.fasterxml.jackson.databind.ObjectMapper;

/* This  class will write the transformed records to ES using Bulk API
 * 
 * @author neha_jain
 * 
 */
public class ESWriter {
	
	public static StringBuilder bulkEvents = new StringBuilder();
	private static int count=0, buffer_threshold =0;
	private static String esHost,schemaFilePath, type ;
	private static boolean createIndex=false;
	private static List<String> indexes= new ArrayList<String>();
	private static PropertiesHandler phandler= null;
	private static ElasticSearchRepo repository=null;
	
	final private static Logger logger = Logger.getLogger(ESWriter.class);
	
	public static void init(String esFilePath){
		try {
			phandler=  new PropertiesHandler(esFilePath);
			esHost= phandler.getValue("urlHost");
			type=phandler.getValue("insert.type");
			createIndex= Boolean.parseBoolean(phandler.getValue("create.index"));
			buffer_threshold=Integer.parseInt(phandler.getValue("count.threshold"));
			schemaFilePath=phandler.getValue("schemaPath");
			repository=new ElasticSearchRepo(esHost);
		} catch (IOException e) {
			logger.fatal("Error in reading from properties file");
		}
	}
	
	public void writeToES(String index, String id, String parentId, Object entity){
		++count;
		try{
			if(Integer.compare(buffer_threshold, count)==0){
				processData(index, id, parentId, entity);
				logger.info("Posting data"+bulkEvents.toString());
				postBulkData(bulkEvents.toString());
				count=0;
				bulkEvents=null;
			}
			else{
				processData(index, id, parentId, entity);
				}
			}
		catch (Exception e) {
			logger.error("Error in processing bulk events");
		}
	}
	
	private void processData(String index, String id, String parentId, Object entity) throws Exception {
		JSONObject obj=null;
		boolean deleted;
		ObjectMapper mapper = new ObjectMapper();
		String entityJson= null;
	
		if(parentId!=null)
			id= parentId +"-"+id;		
	
		if(bulkEvents==null)
			bulkEvents = new StringBuilder();
		
		if(createIndex){
			if(!indexes.contains(index)){
				deleted= repository.deleteESIndexIfExists(index, esHost);
			if(deleted)
				repository.createESIndex(index, esHost);
			indexes.add(index);
			repository.addESSchemaMapping(index, type, schemaFilePath, esHost);
		   }
		}
		if(parentId!=null)
			obj= getObj(index, type, id, "create", parentId);
		else
			obj= getObj(index, type, String.valueOf(id), "create", null);
		entityJson = mapper.writeValueAsString(entity);
			
		if(obj!=null & entityJson!=null)
			bulkEvents.append(obj.toString()+"\n"+entityJson+"\n");
		
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
	
	public void postBulkData(String json) {
		try {
			repository.doHttpRequest(esHost+"_bulk", json, "POST", true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
