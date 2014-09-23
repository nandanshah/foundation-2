package com.dla.foundation.connector.persistence.elasticsearch;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkFiles;
import org.json.JSONObject;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.connector.data.cassandra.CassandraEntityReader;
import com.dla.foundation.connector.model.RecoType;
import com.dla.foundation.connector.util.PropKeys;
import com.dla.foundation.connector.util.StaticProps;
import com.fasterxml.jackson.databind.ObjectMapper;

/* This  class will write the transformed records to ES using Bulk API
 * 
 * @author neha_jain
 * 
 */
public class ESWriter implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static StringBuilder bulkEvents = new StringBuilder(); 
	private static int count=0;
	public static int buffer_threshold =0;
	public static String esHost,schemaFilePath, type ;
	public static boolean createIndex=false;
	private static List<String> indexes= new ArrayList<String>();
	private static PropertiesHandler phandler= null;
	private String JSON_FILE_NAME1 = "userRecoSchema_1.json";
	private String JSON_FILE_NAME2 = "userrecoSchema_2.json";
	public static ElasticSearchRepo repository=null;

	final private static Logger logger = Logger.getLogger(ESWriter.class);
	public static RecoType reco_type;
	final static String reco_type_index = "catalog";
	final static String	reco_type_name = "reco_type";
	final static String reco_type_id = "_show";
	final static String catalogIndex = "catalog";
	
	/*
	 * This method is used to initialize the ES specific stuff and it is done at the start of application
	 * It reads the active passive reco type from the reco_type doc stored in ES and stores for further use. 
	 */
	public static void init(String commonFilePath, String path){
		try {
			
			phandler=  new PropertiesHandler(commonFilePath);
			esHost="http://"+phandler.getValue(PropKeys.ES_HOST.getValue())+":"+phandler.getValue(PropKeys.ES_PORT.getValue())+"/";
			repository=new ElasticSearchRepo(esHost);
			checkRecoTypeFromJSON(reco_type_index,reco_type_name,path);
			if(reco_type==null){
				reco_type = new RecoType("user_reco_1","user_reco_2");
				repository.addItem(esHost+reco_type_index+"/"+reco_type_name+"/"+reco_type_id, reco_type);
			}
			type= reco_type.getPassive();//phandler.getValue("insert.type");//reco_type.getPassive();
		
			createIndex= Boolean.parseBoolean(StaticProps.CREATE_INDEX.getValue());
			buffer_threshold=Integer.parseInt(StaticProps.COUNT_THRESHHOLD.getValue());
			if(type.equals("user_reco_1"))
				schemaFilePath=path+StaticProps.SCHEMA_PATH1.getValue();
			else
				{
					if(type.equals("user_reco_2"))
						schemaFilePath=path+StaticProps.SCHEMA_PATH2.getValue();
					else
						logger.error("reco_type is invalid");
				}
			
		} catch (IOException e) {
			logger.fatal("Error in reading from properties file");
		}
	}
	/*
	 * Write records read from cassandra into ES
	 */
	public void writeToES(String index, String id, String parentId, Object entity,Map<String,String> map){
		++count;
		//Should be initialized only once at worker in distributed mode. 
		if(esHost==null){
			initializeVars(map);
		}
		try{
				logger.info("esHost in writeToES "+esHost);
				logger.info("passive type is"+type);
				logger.info("schemaPath in writeToES "+schemaFilePath);
				processData(index, id, parentId, entity);
				logger.info("Posting data"+bulkEvents.toString());
				postBulkData(bulkEvents.toString());
				count=0;
				bulkEvents=null;
			}
		catch (Exception e) {
			logger.error("Error in processing bulk events");
		}
	}
	/*
	 * Initialize the ES specific stuff broadcasted by master in distributed mode.
	 */
	private void initializeVars(Map<String, String> map) {
		// TODO Auto-generated method stub
		logger.info(" To check if initialize required everytime esHost : " + esHost + "repository "+repository);
		esHost = map.get("esHost");
		logger.info("reco type : Active " + map.get("ActiveRecoType")+"Passive is : "+map.get("PassiveRecoType"));
		//reco_type = new RecoType(map.get("ActiveRecoType"),map.get("PassiveRecoType"));
		type = map.get("type");
		createIndex = Boolean.valueOf(map.get("createIndex"));
		buffer_threshold = Integer.parseInt(map.get("buffer_threshold"));
		if(schemaFilePath==null){
			if(type.equals("user_reco_1")){
				schemaFilePath = SparkFiles.get(JSON_FILE_NAME1);
		
			}else{
				schemaFilePath = SparkFiles.get(JSON_FILE_NAME2);
			}
		}
		repository = new ElasticSearchRepo(esHost);	
	}
	/*
	 * Converts the object into json form and creates a string to be written into ES.
	 */
	private void processData(String index, String id, String parentId, Object entity) throws Exception {
		JSONObject obj=null;
		//boolean deleted;
		ObjectMapper mapper = new ObjectMapper();
		String entityJson= null;
	
		if(parentId!=null)
			id= parentId +"-"+id;		
	
		if(bulkEvents==null)
			bulkEvents = new StringBuilder();

		if(createIndex){
			logger.info("repository value "+repository+"catalogIndex is"+ catalogIndex);
			boolean indexTypeExists = repository.checkESIndexTypeIfExists(catalogIndex+"/"+type+"/_mapping", esHost);
			logger.info("index type Exixts or not"+indexTypeExists);
			if(!indexTypeExists)
			{
				logger.info("index type doesnot exists so creating");
				repository.createESIndex(catalogIndex+"/"+type, esHost);
				repository.addESSchemaMapping(catalogIndex, type, schemaFilePath, esHost);
			}
		}
		if(parentId!=null)
			obj= getObj(catalogIndex, type, id, "create", parentId);
		else
			obj= getObj(catalogIndex, type, String.valueOf(id), "create", null);
		logger.info("------entity----- : "+entity);
		entityJson = mapper.writeValueAsString(entity);
		logger.info("------entityJson----- : "+entityJson);
		if(obj!=null & entityJson!=null)
			{
				bulkEvents.append(obj.toString()+"\n"+entityJson+"\n");
				logger.info("user reco appended to bulkEvents "+bulkEvents);
			}
		
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
	/*
	 * Post the data using bulk api into ES.
	 */
	public void postBulkData(String json) {
		try {
			System.out.println("posting bulk requests to this url : " +esHost+"_bulk");
			System.out.println("posting this json : " +json);
			repository.doHttpRequest(esHost+"_bulk", json, "POST", true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/*
	 * Checks if the index and type is present in ES.
	 * @param indexName -index in ES
	 * 		  indexType - type in ES
	 * 		  path  - <ip>:<port>
	 */
	public static void checkRecoTypeFromJSON(String indexName,String indexType,String path){
		boolean indexExists = repository.checkESIndexTypeIfExists(indexName+"/"+indexType+"/"+"_mapping", esHost);
		
			if(!indexExists){
				logger.debug("Creating index "+ indexName);
				boolean indexCreated = repository.createESIndex(indexName+"/"+indexType+"/"+"_show", esHost);
				if(indexCreated)
					indexes.add(indexName);
				else
				{				
					repository.createESIndex(indexName, esHost);
					logger.info("Catalog was not present so created");
				}
				repository.addESSchemaMapping(indexName, indexType, path+StaticProps.SCHEMA_PATH3.getValue(), esHost);
				reco_type = new RecoType("user_reco_1", "user_reco_2");
				try {
					repository.addItem(esHost+indexName+"/"+indexType+"/"+"_show", reco_type);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					System.err.println("Error adding json item for active passive reco-type"+e.getMessage());
				}
			}
			else{
				
				try {
					reco_type = (RecoType)repository.getItem(indexName, indexType, "_show");
					logger.debug("recotype in json"+reco_type);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					System.err.println("Error in getting an item from ES while checking reco type"+e.getMessage());
				}
			}
		
	}
	/*
	 * Swap the active passive reco type and update the reco type doc in ES.
	 */
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
