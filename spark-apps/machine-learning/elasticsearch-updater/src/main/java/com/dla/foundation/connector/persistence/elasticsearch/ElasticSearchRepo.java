package com.dla.foundation.connector.persistence.elasticsearch;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.dla.foundation.connector.model.RecoType;
import com.dla.foundation.connector.model.UserRecommendation;
import com.fasterxml.jackson.databind.ObjectMapper;

/*
 * This class contains rest calls code logic to get/put/post data to ES
 * @author neha_jain
 * 
 */
public class ElasticSearchRepo {

	final private String hostUrl;

	private static Logger logger = Logger.getLogger(ElasticSearchRepo.class);

	public ElasticSearchRepo(String hostUrl) {
		this.hostUrl = hostUrl;
	}
	
	/***
	 * To add Schema Mapping into ES
	 * 
	 * @param url
	 * @param jsonContent
	 */
	public void addESSchemaMapping(String indexName, String indexType,String schemaFileName, String urlHost) {
		String url = "";
		String jsonContent = "";
		try {
			url = urlHost + indexName + "/" + indexType + "/" + "_mapping";

			logger.info("Creating mapping for " + indexType
					+ " index type in ES:" + url + "\n");
			jsonContent = readFile(schemaFileName);
			doHttpRequest(url, jsonContent, "POST", true);

			logger.info("Mapping created");

			
		} catch (MalformedURLException e) {
			System.err.println("Malformed URL Exception at "+url + e.getMessage());
		} catch (IOException e) {			
			System.err.println("IOException for creating mapping at "+url + e.getMessage());
		}

	}
	
	public boolean deleteESIndexTypeIfExists(String indexTypeName, String urlHost) {
		boolean deleted=false;
		try {
			URL url =new URL(urlHost + indexTypeName);
			logger.info("Creating " + indexTypeName + " index in ES:" + url+ "\n");
			
			HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
			httpConnection.setRequestMethod("PUT");
			httpConnection.setRequestProperty("Content-Type", "application/json");
			if(httpConnection.getResponseCode() ==400)
			{   deleted= true;
				logger.info("IndexType exists so deleting");
				deleteItem(url.toString());
				logger.info("IndexType deleted "+indexTypeName);
			} else if(httpConnection.getResponseCode() ==200)
				logger.info("IndexType created " +indexTypeName);
			
		}  catch (IOException e) {
			System.out.println("Error while creating index:");
			e.printStackTrace();
		}
		return deleted;
	}
	/*
	 * Create index in ES with HTTP PUT call
	 */
	public boolean createESIndex(String indexName, String urlHost) {
		boolean exists=false;
		try {
			URL url =new URL(urlHost + indexName);
			logger.info("Creating " + indexName + " index in ES:" + url+ "\n");
			
			HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
			httpConnection.setRequestMethod("PUT");
			httpConnection.setRequestProperty("Content-Type", "application/json");
			if(httpConnection.getResponseCode() ==200){
				logger.info("Index created " +indexName);
				exists=true;
			}
			
		} catch (IOException e) {
			System.err.println("Error while creating index:" + indexName + e.getMessage());
		}
		return exists;
	}
	
	public ElasticSearchResult deleteItem(String urlString)  throws IOException {
		URL url = new URL(urlString);

		HttpURLConnection conn;

		conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("DELETE");
		ElasticSearchResult esResult;
		ObjectMapper mapper = new ObjectMapper();
		try (InputStream inputStream = conn.getInputStream()) {
			esResult = mapper.readValue(inputStream, ElasticSearchResult.class);
		}

		return esResult;
	}

	public String readFile(String fileName) throws IOException {
		File file = new File(fileName);
		FileReader reader = new FileReader(file);
		BufferedReader buffRead = new BufferedReader(reader);
		String line = null;
		String string = "";
		while ((line = buffRead.readLine()) != null) {
			string = string + line;
		}
		buffRead.close();
		return string;
	}

	public ElasticSearchResult updateItem(String urlString, Object entity) throws IOException{
		deleteItem(urlString);
		ElasticSearchResult es= addItem(urlString, entity);
		return es;
	}
	
	public UserRecommendation getUserRecoItem(String index, String type, String id, String parent_id) throws IOException {
		
		URL url = generateUrl(index, type, id, parent_id);
		HttpURLConnection conn;

		conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");

		ObjectMapper mapper = new ObjectMapper();
		UserRecoResult uResult=null;	
				
		try (InputStream inputStream = conn.getInputStream()) {
			uResult = mapper.readValue(inputStream, UserRecoResult.class);

		} catch (FileNotFoundException e) {
				return null;
		}
		
		return uResult._source;
	}
	
	protected URL generateUrl(String index, String type, String id, String parentid) throws MalformedURLException {
		StringBuilder urlString = new StringBuilder();
		urlString.append(String.format("%s/%s", hostUrl, index));
		if (type!=null){
			urlString.append("/" + type);
		}
		if (id!=null){
			urlString.append("/" + id);
		}
		if (parentid!=null){
			urlString.append("?parent=" +parentid );
		}
		URL url = new URL(urlString.toString());
		return url;
	}
	
	
	public String doHttpRequest(String url, String jsonContent,String requestMethod, boolean writeToHttpStream)	throws IOException {
		URL targetUrl = new URL(url);
		HttpURLConnection httpConnection = (HttpURLConnection) targetUrl.openConnection();
		httpConnection.setDoOutput(writeToHttpStream);
		httpConnection.setRequestMethod(requestMethod);
		httpConnection.setRequestProperty("Content-Type", "application/json");
		if (writeToHttpStream == true) {
			OutputStream outputStream = httpConnection.getOutputStream();
			outputStream.write(jsonContent.getBytes());
			outputStream.flush();

			if (httpConnection.getResponseCode() != 200) {
				//logWriter.write("Failed : HTTP error code : "	+ httpConnection.getResponseCode());
				throw new RuntimeException("Failed : HTTP error code : "+ httpConnection.getResponseCode());
			}
		}

		BufferedReader responseBuffer = new BufferedReader(new InputStreamReader((httpConnection.getInputStream())));
		String output;
		String result = "";

		logger.info("Server Response:");
		while ((output = responseBuffer.readLine()) != null) {
			System.out.println(output);
			result = result + output;
		}

		httpConnection.disconnect();
		return result;
	}
	
	/*
	 * Does a HTTP GET call to check if index/type exists. 
	 */
	public boolean checkESIndexTypeIfExists(String indexTypename, String urlHost){
		try {
			logger.info("urlHost and indexTypeName"+urlHost+"indextype name"+indexTypename);
			URL url =new URL(urlHost + indexTypename);
			HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
			httpConnection.setRequestMethod("GET");
			if(httpConnection.getResponseCode() ==200){
				BufferedReader in = new BufferedReader(new InputStreamReader(
		                                    httpConnection.getInputStream()));
		        String inputLine;
		        while ((inputLine = in.readLine()) != null) {
		            System.out.println("Mapping is : "+inputLine);
		            if(inputLine.length()>2)
		            	return true;
		        }
			}else if(httpConnection.getResponseCode() ==400)
				return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("error in checking index/type at: "+indexTypename + e.getMessage());
		}
		return false;
	}
	/*
	 * Add an item to ES using HTTP POST call 
	 * @param urlString
	 * @param entity
	 */
	public ElasticSearchResult addItem(String urlString, Object entity) throws IOException{
		System.out.println("url"+urlString);
		URL url = new URL(urlString);
		System.out.println("urlstring"+urlString);
		ObjectMapper mapper = new ObjectMapper();
		HttpURLConnection conn = null;
		conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("POST");
		conn.setDoOutput(true);
		
		String entityJson = mapper.writeValueAsString(entity);
		logger.info("addItem: " + entityJson);
		try (OutputStream outputStream = conn.getOutputStream()) {
		
			mapper.writeValue(outputStream, entity);
		}catch (Exception e) {
			// TODO: handle exception
			System.err.println("Error in adding an item to ES at : "+url+e.getMessage());
		}
		ElasticSearchResult esResult = null;
		try (InputStream inputStream = conn.getInputStream()) {
				esResult = mapper.readValue(inputStream, ElasticSearchResult.class);
		}catch (Exception e) {
			// TODO: handle exception
			System.err.println("Error in reading an item from ES at : "+url+e.getMessage());
		}

		return esResult;
	}
	
	/*
	 * Get an item from ES using a HTTP GET call
	 */	
	public RecoType getItem(String index, String type, String id) throws IOException {
		URL url = generateUrl(index, type, id, null);
		HttpURLConnection conn;

		conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");

		ObjectMapper mapper = new ObjectMapper();
		RecoTypeResult uResult=null;	
				
		try (InputStream inputStream = conn.getInputStream()) {
			uResult = mapper.readValue(inputStream, RecoTypeResult.class);

		} catch (FileNotFoundException e) {
				return null;
		}
		
		return uResult._source;
	}
	
}