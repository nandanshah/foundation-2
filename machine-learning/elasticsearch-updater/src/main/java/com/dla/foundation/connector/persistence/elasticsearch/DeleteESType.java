package com.dla.foundation.connector.persistence.elasticsearch;

import java.io.IOException;
import org.apache.log4j.Logger;
import com.dla.foundation.analytics.utils.PropertiesHandler;

public class DeleteESType {
	
	private static String esHost, userRecoType ;
	PropertiesHandler phandler= null;
	ElasticSearchRepo repository=null;
	final private Logger logger = Logger.getLogger(DeleteESType.class);
	
	public DeleteESType(){
		
	}
	
	private void init(String propertiesFilePath){
		try {
			phandler=  new PropertiesHandler(propertiesFilePath);
			esHost= phandler.getValue("urlHost");
			userRecoType=phandler.getValue("delete.user.reco.type");
			repository= new ElasticSearchRepo(esHost);
		} catch (IOException e) {
			logger.error("Error in reading from properties file while deleting reco types");
		}
	}
	
	public  void deleteType(String filePath)  {
		init(filePath);
		String urlString= esHost + "_all" + "/" + userRecoType;
		try {
			repository.deleteItem(urlString);
			logger.info("User Reco Type " +userRecoType+ " deleted from ES");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

}
