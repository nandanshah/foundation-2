package com.dla.foundation.connector;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.connector.data.cassandra.CassandraEntityReader;
import com.dla.foundation.connector.persistence.elasticsearch.DeleteESType;

public class ConnectorDriver 
{
	private static Logger logger = Logger.getLogger(ConnectorDriver.class);

    public static void main(String[] args)
    {
      if(args.length==3){
    	 ConnectorDriver driver= new ConnectorDriver();
    	 driver.run(args);
      }
      else
    	  System.err.println("USAGE: ConnectorDriver apppropertiesfile userrecopropfile elasticsearchpropfile");
    }
    
    
    public void run(String[] args){
    	CassandraEntityReader userReco= new CassandraEntityReader();
    	DeleteESType deleteTypes= new DeleteESType();
    	String app_mode=null;
    	
    	try {
			PropertiesHandler phandler = new PropertiesHandler(args[0]);
			
			app_mode= phandler.getValue("app.mode");

			if(app_mode.equals("insert")){
				logger.info("Satrting Connector- Reading records from Cassandra and writing to ES");
				userReco = new CassandraEntityReader();
	    		userReco.runUserRecoDriver( args[0], args[1], args[2]);
	    	}
			
			else if (app_mode.equals("delete")){
				logger.info("Deleting user reco type from all indexes");
				deleteTypes.deleteType(args[2]);
			}
			
		} 
    	catch (IOException e) {
    		logger.fatal("Error getting properties file", e);
    	}
    }
 }
