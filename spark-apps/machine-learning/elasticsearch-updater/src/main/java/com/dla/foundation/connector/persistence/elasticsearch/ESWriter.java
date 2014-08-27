package com.dla.foundation.connector.persistence.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.connector.model.RecoType;
import com.dla.foundation.connector.util.PropKeys;
import com.dla.foundation.connector.util.StaticProps;
import com.dla.foundation.services.contentdiscovery.entities.MediaItem;
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
	public static RecoType reco_type;
	final static String reco_type_index = "catalog";
	final static String	reco_type_name = "reco_type";
	final static String reco_type_id = "_show";
	final static String catalogIndex = "catalog";
	
	public static void init(String commonFilePath){
		try {
			
			phandler=  new PropertiesHandler(commonFilePath);
			esHost="http://"+phandler.getValue(PropKeys.ES_HOST.getValue())+":"+phandler.getValue(PropKeys.ES_PORT.getValue())+"/";
			repository=new ElasticSearchRepo(esHost);
			checkRecoTypeFromJSON(reco_type_index,reco_type_name);
			type= reco_type.getPassive();//phandler.getValue("insert.type");//reco_type.getPassive();
			createIndex= Boolean.parseBoolean(StaticProps.CREATE_INDEX.getValue());
			buffer_threshold=Integer.parseInt(StaticProps.COUNT_THRESHHOLD.getValue());
			if(type.equals("user_reco_1"))
				schemaFilePath=StaticProps.SCHEMA_PATH1.getValue();
			else
				{
					if(type.equals("user_reco_2"))
						schemaFilePath=StaticProps.SCHEMA_PATH2.getValue();
					else
						logger.error("reco_type is invalid");
				}
			
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
		//boolean deleted;
		ObjectMapper mapper = new ObjectMapper();
		String entityJson= null;
	
		if(parentId!=null)
			id= parentId +"-"+id;		
	
		if(bulkEvents==null)
			bulkEvents = new StringBuilder();
		
		/*if(createIndex){
			if(!indexes.contains(index)){		// index = catalog 
				deleted= repository.deleteESIndexIfExists(index, esHost);
			if(deleted)
				repository.createESIndex(index, esHost);
			indexes.add(index);
			repository.addESSchemaMapping(index, type, schemaFilePath, esHost);
		   }
		}*/
		if(createIndex){
			boolean indexExists = repository.checkESIndexIfExists(catalogIndex, esHost);
			if(indexExists)
			{
				repository.addESSchemaMapping(catalogIndex, type, schemaFilePath, esHost);
			}
			else
			{
				repository.createESIndex(catalogIndex, esHost);
			}
		}
		if(parentId!=null)
			obj= getObj(catalogIndex, type, id, "create", parentId);
		else
			obj= getObj(catalogIndex, type, String.valueOf(id), "create", null);
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
	
	public static void checkRecoTypeFromJSON(String indexName,String indexType){
		boolean indexExists = repository.checkESIndexIfExists(indexName, esHost);
		
			if(!indexExists){
				logger.debug("Creating index "+ indexName);
				repository.createESIndex(indexName+"/"+indexType+"/"+"_show", esHost);
				indexes.add(indexName);
				repository.addESSchemaMapping(indexName, indexType, "src/main/resources/Reco_type.json", esHost);
				reco_type = new RecoType("user_reco_1", "user_reco_2");
				try {
					
					repository.addItem(esHost+indexName+"/"+indexType+"/"+"_show", reco_type);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					logger.error("Error adding json item for active passive reco-type");
				}
			}
			else{
				
				try {
					reco_type = (RecoType)repository.getItem(indexName, indexType, "_show");
					logger.debug("recotype in json"+reco_type);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		
	}
	
	public static void swapRecoType(){
		String active = reco_type.getActive();
		String passive = reco_type.getPassive();
		reco_type.setActive(passive);
		reco_type.setPassive(active);
		try {
			repository.updateItem(esHost + reco_type_index + "/" + reco_type_name +"/"+reco_type_id, reco_type);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
