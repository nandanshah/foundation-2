package com.dla.foundation.data.persistence.elasticsearch;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dla.foundation.data.entities.analytics.AnalyticsCollectionEvent;
import com.dla.foundation.services.contentdiscovery.entities.MediaItem;
import com.dla.foundation.analytics.utils.*;

public class ESServiceImpl implements ESService {

	final Logger logger = Logger.getLogger(this.getClass());
	public static ElasticSearchRepo repository=null;
	private static String index, esHost, movieType, userRecoType;
	private static PropertiesHandler phandler= null;
		
	public ESServiceImpl(PropertiesHandler handler) throws IOException{
			phandler=handler;
			esHost= phandler.getValue("urlHost");
			repository = new ElasticSearchRepo(esHost);
			index=phandler.getValue("index_name");
			movieType=phandler.getValue("movie_index_type");
			userRecoType=phandler.getValue("userreco_index_type");
			System.out.println("Host"+esHost);
	}
	

	@Override
	public void addItem(AnalyticsCollectionEvent event) throws IOException {
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
	public void deleteItem(AnalyticsCollectionEvent event) throws IOException {
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
	public void updateItem(AnalyticsCollectionEvent event) throws IOException {
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
		
	}

	@Override
	public void postBulkEvents(String json) {
		try {
			repository.doHttpRequest(esHost+"_bulk", json, "POST", true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
