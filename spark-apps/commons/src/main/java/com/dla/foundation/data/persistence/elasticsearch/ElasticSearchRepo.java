package com.dla.foundation.data.persistence.elasticsearch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.eclipse.jetty.http.HttpMethods;

import com.dla.foundation.analytics.utils.HttpDeleteWithBody;
import com.dla.foundation.services.contentdiscovery.entities.MediaItem;
import com.dla.foundation.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ElasticSearchRepo {

	final private String hostUrl;

	private static Logger logger = Logger.getLogger(ElasticSearchRepo.class);

	public ElasticSearchRepo(String hostUrl) {
		this.hostUrl = hostUrl;
	}

	public ElasticSearchResult addItem(String urlString, Object entity) throws IOException{
		URL url = new URL(urlString);
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

	public ElasticSearchResult deleteItem(String  urlString) throws IOException{
		URL url = new URL(urlString);

		HttpURLConnection conn;

		conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("DELETE");
		//		conn.setDoOutput(true);
		ElasticSearchResult esResult;
		ObjectMapper mapper = new ObjectMapper();
		try (InputStream inputStream = conn.getInputStream()) {
			esResult = mapper.readValue(inputStream, ElasticSearchResult.class);
		}

		return esResult;
	}

	public ElasticSearchResult updateItem(String urlString, Object entity) throws IOException{
		deleteItem(urlString);
		ElasticSearchResult es= addItem(urlString, entity);
		return es;
	}

	public MediaItem getMediaItem(String index, String type, String id) throws IOException {
		URL url = generateUrl(index, type, id, null);
		HttpURLConnection conn;

		conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");

		ObjectMapper mapper = new ObjectMapper();
		MovieItemResult mResult=null;	

		try (InputStream inputStream = conn.getInputStream()) {
			mResult = mapper.readValue(inputStream, MovieItemResult.class);

		} catch (FileNotFoundException e) {
			return null;
		}

		return mResult._source;
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
		if (!StringUtils.isNullOrEmpty(type)){
			urlString.append("/" + type);
		}
		if (!StringUtils.isNullOrEmpty(id)){
			urlString.append("/" + id);
		}
		if (!StringUtils.isNullOrEmpty(parentid)){
			urlString.append("?parent=" +parentid );
		}
		URL url = new URL(urlString.toString());
		return url;
	}


	public String doHttpRequest(String url, String jsonContent, String requestMethod, boolean writeToHttpStream) throws IOException {

		HttpClient client = new DefaultHttpClient();
		HttpResponse response = null;

		if(requestMethod.equals(HttpMethods.GET)) {
			HttpGet method = new HttpGet(url);
			response = client.execute(method);
		} else if(requestMethod.equals(HttpMethods.POST)) {
			HttpPost method = new HttpPost(url);
			if (writeToHttpStream) {
				HttpEntity entity = new ByteArrayEntity(jsonContent.getBytes("UTF-8"));
				method.setEntity(entity);
			}
			response = client.execute(method); 
		} else if(requestMethod.equals(HttpMethods.DELETE)) {
			HttpDeleteWithBody method = new HttpDeleteWithBody(url);
			if (writeToHttpStream) {
				HttpEntity entity = new ByteArrayEntity(jsonContent.getBytes("UTF-8"));
				method.setEntity(entity);
			}
			response = client.execute(method);
		}

		StatusLine s = response.getStatusLine();
		int statusCode = s.getStatusCode();

		if (statusCode != HttpStatus.SC_OK) {
			throw new RuntimeException("Failed : HTTP error code : " + s.getReasonPhrase());
		}

		return EntityUtils.toString(response.getEntity());
	}
}