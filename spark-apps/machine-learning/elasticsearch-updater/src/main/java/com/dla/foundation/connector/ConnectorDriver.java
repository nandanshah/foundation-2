package com.dla.foundation.connector;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dla.foundation.analytics.utils.PropertiesHandler;
import com.dla.foundation.connector.data.cassandra.CassandraEntityReader;
import com.dla.foundation.connector.persistence.elasticsearch.DeleteESType;
import com.dla.foundation.connector.persistence.elasticsearch.ESWriter;

public class ConnectorDriver 
{
	private static Logger logger = Logger.getLogger(ConnectorDriver.class);

    public static void main(String[] args)
    {
      if(args.length==1){
    	 ConnectorDriver driver= new ConnectorDriver();
    	 driver.run(args);
      }
      else
    	  System.err.println("USAGE: ConnectorDriver takes 1 argument of common propertis file");
    }
    
    
    public void run(String[] args){
    	CassandraEntityReader userReco= new CassandraEntityReader();
    	DeleteESType deleteTypes= new DeleteESType();
    	//String app_mode=null;
    	
    	//app_mode= phandler.getValue("app.mode");
		ESWriter.init(args[0]);
		/*logger.info("Deleting user reco type from all indexes");
		deleteTypes.deleteType(args[0]);*/
		//if(app_mode.equals("insert")){
			logger.info("Satrting Connector- Reading records from Cassandra and writing to ES");
			try {
				userReco.runUserRecoDriver(args[0]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		//}
		
		//else if (app_mode.equals("delete")){
			/*logger.info("Deleting user reco type from all indexes");
			deleteTypes.deleteType(args[2]);*/
		//}
    }
 }
