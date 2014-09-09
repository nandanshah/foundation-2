package com.dla.foundation.data.persistence.elasticsearch;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.eclipse.jetty.http.HttpMethods;
import org.json.JSONObject;

import com.dla.foundation.services.contentdiscovery.entities.MediaItem;
import com.dla.foundation.analytics.utils.*;
import com.dla.foundation.data.entities.event.Event;

public class ESServiceImpl implements ESService {

	final Logger logger = Logger.getLogger(this.getClass());

	public static ElasticSearchRepo repository = null;
	private static String index, esHost, esPort, movieType, userRecoType;
	private static PropertiesHandler phandler = null;
	private static String activeReco;

	public ESServiceImpl(PropertiesHandler handler) throws IOException {
		phandler = handler;
		esHost = phandler.getValue(CommonPropKeys.es_host);
		esPort = phandler.getValue(CommonPropKeys.es_httpPort);
		repository = new ElasticSearchRepo(esHost);
		index = phandler.getValue(CommonPropKeys.es_index_name);
		movieType = phandler.getValue(CommonPropKeys.es_movie_index_type);
		userRecoType = phandler.getValue(CommonPropKeys.es_userreco_index_type);
		logger.info("Host" + esHost);
	}

/*	@Override
	public void addItem(Event event) throws IOException {
		if(event.customEventLabel.equals(phandler.getValue("movie.add"))){   
			MediaItem mediaItem = new MediaItem();
			mediaItem.id = event.customEventValue;
			String movieUrl= esHost + index + "/" + movieType + "/" + mediaItem.id;
			try {
				repository.addItem(movieUrl, mediaItem);
				logger.info("Movie "+mediaItem.id +"added to ES");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if(event.customEventLabel.equals(phandler.getValue("userReco.add"))){
			UserRecommendation userreco= new UserRecommendation();
			userreco.userid= event.customEventValue;
			String recoUrl= esHost + index + "/" + userRecoType + "/" + userreco.userid+ "?parent=" + event.linkId;
			try {
				repository.addItem(recoUrl, userreco);
				logger.info("User Reco "+userreco.userid +"added to ES");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void deleteItem(Event event) throws IOException {
		if(event.customEventLabel.equals(phandler.getValue("movie.delete"))){   
			MediaItem mediaItem = new MediaItem();
			mediaItem.id = event.customEventValue;
			String movieUrl= esHost + index + "/" + movieType + "/" + mediaItem.id;
			try {
				repository.deleteItem(movieUrl);
				logger.info("Movie "+mediaItem.id +"deleted from ES");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if(event.customEventLabel.equals(phandler.getValue("userReco.delete"))){
			UserRecommendation userreco= new UserRecommendation();
			userreco.userid= event.customEventValue;
			String recoUrl= esHost + index + "/" + userRecoType + "/" + userreco.userid+ "?parent=" + event.linkId;
			try {
				repository.deleteItem(recoUrl);
				logger.info("User Reco "+userreco.userid +"deleted from ES");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public void updateItem(Event event) throws IOException {
		if(event.customEventLabel.equals(phandler.getValue("movie.update"))){   
			MediaItem mediaItem = new MediaItem();
			mediaItem.id = event.customEventValue;
			mediaItem.title=event.customMetric;
			String movieUrl= esHost + index + "/" + movieType + "/" + mediaItem.id;
			try {
				repository.updateItem(movieUrl, mediaItem);
				logger.info("Movie "+mediaItem.id +"deleted from ES");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if(event.customEventLabel.equals(phandler.getValue("userReco.update"))){
			UserRecommendation userreco= new UserRecommendation();
			userreco.userid= event.customEventValue;
			String recoUrl= esHost + index + "/" + userRecoType + "/" + userreco.userid+ "?parent=" + event.linkId;
			try {
				repository.updateItem(recoUrl, userreco);
				logger.info("User Reco "+userreco.userid +"deleted from ES");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}*/

	@Override
	public void postBulkEvents(String json) {
		try {
			repository.doHttpRequest(esHost + "_bulk", json, HttpMethods.POST, true);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public void deleteUserReco(String filterJson) {
		try {
			JSONObject json = new JSONObject(repository.doHttpRequest("http://" + esHost + ":" + esPort + "/" + index + "/reco_type/_show", null, HttpMethods.GET, false));
			activeReco = json.getJSONObject("_source").getString("active");
			String resp  = repository.doHttpRequest("http://" + esHost + ":" + esPort + "/" + index + "/" + activeReco + "/_query", filterJson, HttpMethods.DELETE, true);
			logger.info(resp);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}
}
