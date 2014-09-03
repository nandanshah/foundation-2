package com.dla.foundation.connector.persistence.elasticsearch;

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

import org.apache.log4j.Logger;

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
			e.printStackTrace();
		} catch (IOException e) {			
			e.printStackTrace();
		}

	}
	
	public boolean deleteESIndexIfExists(String indexName, String urlHost) {
		boolean deleted=false;
		try {
			URL url =new URL(urlHost + indexName);
			logger.info("Creating " + indexName + " index in ES:" + url+ "\n");
			
			HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
			httpConnection.setRequestMethod("PUT");
			httpConnection.setRequestProperty("Content-Type", "application/json");
			if(httpConnection.getResponseCode() ==400)
			{   deleted= true;
				logger.info("Index exists so deleting");
				deleteItem(url.toString());
				logger.info("Index deleted "+indexName);
			} else if(httpConnection.getResponseCode() ==200)
				logger.info("Index created " +indexName);
			
		}  catch (IOException e) {
			System.out.println("Error while creating index:");
			e.printStackTrace();
		}
		return deleted;
	}

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
			System.out.println("Error while creating index:");
			e.printStackTrace();
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
	
	public boolean checkESIndexIfExists(String indexName, String urlHost) {
		try {
			URL url =new URL(urlHost + indexName);
			//logger.info("Creating " + indexName + " index in ES:" + url+ "\n");
			
			HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
			httpConnection.setRequestMethod("PUT");
			httpConnection.setRequestProperty("Content-Type", "application/json");
			if(httpConnection.getResponseCode() ==400)
			{   return true;
			} else if(httpConnection.getResponseCode() ==200)
				return false;
			
		}  catch (IOException e) {
			System.out.println("Error while creating index:");
			e.printStackTrace();
		}
		return false;
	}
	
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
		}
		ElasticSearchResult esResult;
		try (InputStream inputStream = conn.getInputStream()) {
				esResult = mapper.readValue(inputStream, ElasticSearchResult.class);
		}

		return esResult;
	}
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