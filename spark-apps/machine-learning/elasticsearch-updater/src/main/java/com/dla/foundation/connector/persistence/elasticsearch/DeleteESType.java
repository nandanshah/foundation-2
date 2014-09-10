package com.dla.foundation.connector.persistence.elasticsearch;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.connector.util.PropKeys;

/*
 * This class will be invoked when running app in delete mode. It will delete 'user_reco' type 
 * from all indexes in ES
 * @author neha_jain
 */
public class DeleteESType {
	
	private static String esHost, userRecoType ;
	PropertiesHandler phandler= null;
	ElasticSearchRepo repository=null;
	final private Logger logger = Logger.getLogger(DeleteESType.class);
	
	private void init(String propertiesFilePath){
		try {
			phandler=  new PropertiesHandler(propertiesFilePath);
			esHost= "http://"+phandler.getValue(PropKeys.ES_HOST.getValue())+":"+phandler.getValue(PropKeys.ES_PORT.getValue())+"/";//phandler.getValue("urlHost");
			userRecoType=	ESWriter.reco_type.getPassive();//phandler.getValue("delete.user.reco.type");
			repository= new ElasticSearchRepo(esHost);
		} catch (IOException e) {
			logger.error("Error in reading from properties file while deleting reco types");
		}
	}
	
	public  void deleteType(String filePath)  {
		init(filePath);
		String urlString= esHost + ESWriter.catalogIndex + "/" + userRecoType;
		try {
			if(repository.checkESIndexTypeIfExists(ESWriter.catalogIndex + "/" + userRecoType + "/"+"_mapping",esHost)){
				repository.deleteItem(urlString);
				logger.info("User Reco Type " +userRecoType+ " deleted from ES");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

}
